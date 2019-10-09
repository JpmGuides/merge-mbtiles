
import { promisify } from 'util';
import * as tilelive from "@mapbox/tilelive";
import { registerProtocols } from '@mapbox/mbtiles';
import { Database, OPEN_READWRITE } from 'sqlite3';
import * as archiver from 'archiver';
import * as unzipper from 'unzipper';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import * as stream from 'stream';

registerProtocols(tilelive);

const sources: { [key: string]: string } = {
  'openmaptiles': 'map.mbtiles',
  'map': 'map.mbtiles',
  'hillshading': 'hillshading.mbtiles',
  'contours': 'contours.mbtiles',
  'terrain-rgb': 'terrain-rgb.mbtiles',
  'landcover': 'landcover.mbtiles'
};

function load<T>(uri: string) : Promise<T> {
  return new Promise((resolve, reject) => {
    tilelive.load(uri, (err: Error | string, result: T) => {
      if (err) { reject(err); } else { resolve(result); }
    });
  });
}

function loadSource(uri: string): Promise<Tilesource> {
  return load<Tilesource>(uri);
}
function loadSink(uri: string): Promise<Tilesink> {
  return load<Tilesink>(uri);
}

function copy(src: Tilesource, dest: Tilesink, options: CopyOptions): Promise<void> {
  return new Promise((resolve, reject) => {
    tilelive.copy(src, dest, options, (err: Error | string) => {
      if (err) { reject(err); } else { resolve(); }
    });
  });
}

function close(tilestore: Tilesink | Tilesource): Promise<void> {
  return new Promise((resolve, reject) => {
    tilestore.close((err?: Error | string) => {
      if (err) { reject(err); } else { resolve(); }
    });
  });
}

async function copyMaps(input: string[], output: string): Promise<void> {
  const destDb = new Database(output, OPEN_READWRITE);
  const runDest: (...args: string[]) => Promise<void> = promisify(destDb.run.bind(destDb));

  await runDest('CREATE TABLE IF NOT EXISTS jpm_map (id NOT NULL UNIQUE, json)');

  for (let file of input) {
    const sourceDb = new Database(file, OPEN_READWRITE);

    await new Promise((resolve, reject) => {
      sourceDb.each(
        "SELECT id, json from jpm_map",
        async function(err: Error, row: {id: string, json: string}) {
          if (err) {
            return;
          }

          try {
            console.log('Adding map for article: ' + row.id);
            await runDest('INSERT OR REPLACE INTO jpm_map(id, json) VALUES (?, ?)',
                      row.id, row.json);
          } catch (err) {
            console.warn('Failed to insert map into ' + output + ': ', err);
          }
        },
        function(err: Error, numrows: number) {
          // an error is OK: there might be no maps or no jpm_map table.
          resolve();
        });
    });
    await (promisify(sourceDb.close.bind(sourceDb)))();
  }

  console.log('Cleaning up...');
  await runDest('VACUUM');
  await (promisify(destDb.close.bind(destDb)))();
}


async function mergeMbtiles(input: string[], output: string): Promise<void> {
  const sinkUri = 'mbtiles://' + output;
  const sink = await loadSink(sinkUri);
  for (let file of input) {
    const uri = 'mbtiles://' + file;
    console.log('Copying tiles from: ' + uri + '...');
    const source = await loadSource(uri);
    await copy(source, sink, {
      type: 'list',
      close: false
    });

    console.log('Done copying tiles from: ' + uri + '.');
    await close(source);
  }

  await close(sink);

  await copyMaps(input, output);
}

async function mktmpdir(): Promise<string> {
  return new Promise((resolve, reject) => {
    fs.mkdtemp(path.join(os.tmpdir(), 'mergezip-'), (err, folder: string) => {
      if (err) {
        reject(err);
      } else{
        resolve(folder);
      }
    });
  });
}

function randomString() {
  return Math.random().toString(36).substring(2);
}

async function unzipFile(compressedFile: string, destFolder: string): Promise<string[]> {
  let result: string[] = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(compressedFile)
      .pipe(unzipper.Parse())
      .pipe(new stream.Transform({
        objectMode: true,
        transform: (entry: any, e, cb: () => void) => {
          const fileName = entry.path;
          const type = entry.type; // 'Directory' or 'File'
          const size = entry.vars.uncompressedSize; // There is also compressedSize;
          if (fileName.replace('.mbtiles', '') in sources) {
            const dest = destFolder + '/' + randomString() + '-' + fileName;
            result.push(dest);
            entry.pipe(fs.createWriteStream(dest))
              .on('finish', cb)
              .on('error', reject);
          } else {
            entry.autodrain();
            cb();
          }
        }
      }))
      .on('finish', () => { resolve(result); })
      .on('error', reject);
  });
}

async function mergeZips(input: string[], outputfilename: string): Promise<void> {
  const inputBySource: { [source: string]: string[] } = { };

  const folder: string = await mktmpdir();

  // Decompress all inputs.
  for (let file of input) {
    const decompressed = await unzipFile(file, folder);
    for (let d of decompressed) {
      const re = new RegExp('(' + Object.keys(sources).map((x) => `(${x})`).join('|') + ').mbtiles')
      const m = d.match(re);
      if (m && m[1] in sources) {
        inputBySource[m[1]] = (inputBySource[m[1]] || []).concat([d]);
      } else {
        throw new Error('Unknown file: ' + d);
      }
    }
  }

  const file = (source: string) => { return `${folder}/${source}.mbtiles`; };

  await Promise.all(
    Object.keys(inputBySource).map((source) => mergeMbtiles(inputBySource[source], file(source))));

  await new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outputfilename);

    const archive = archiver('zip', {
      zlib: { level: 5 } // Sets the compression level.
    });

    output.on('close', resolve);
    archive.on('warning', (err) => { console.warn(err); });
    archive.on('error', (err) => { console.warn(err); reject(err); });
    archive.pipe(output);
    for (let source of Object.keys(inputBySource)) {
      archive.file(file(source), {name: sources[source]});
    }
    archive.finalize();
  });

  // cleanup
  const deleteFile = promisify(fs.unlink);

  // delete merge temporary files (now zipped)
  await Promise.all(Object.keys(inputBySource).map(file).map((fn) => deleteFile(fn)));
  for (let s of Object.keys(inputBySource)) {
    await Promise.all(inputBySource[s].map((fn) => deleteFile(fn)));
  }
  fs.rmdir(folder, () => {});
}

export async function mergeMaps(input: string[], output: string): Promise<void> {
  if (output.match(/zip$/)) {
    return mergeZips(input, output);
  } else {
    return mergeMbtiles(input, output);
  }
}
