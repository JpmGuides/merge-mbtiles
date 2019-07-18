
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
            return console.warn("Can't read map from " + input + ': ', err);
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
          if (err) {
            console.warn("Can't read map from " + input + ': ', err);
          }
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
          if (fileName === "map.mbtiles" || fileName == 'hillshading.mbtiles') {
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
  const inputVecTiles: string[] = [];
  const inputHillshading: string[] = [];

  const folder: string = await mktmpdir();

  // Decompress all inputs.
  for (let file of input) {
    const decompressed = await unzipFile(file, folder);
    for (let d of decompressed) {
      if (d.match(/map.mbtiles$/)) {
        inputVecTiles.push(d);
      } else if (d.match(/hillshading.mbtiles/)) {
        inputHillshading.push(d);
      } else {
        throw new Error('Unknown file: ' + d);
      }
    }
  }

  const vt_file = folder + '/map.mbtiles';
  const hs_file = folder + '/hillshading.mbtiles';
  await Promise.all([
    mergeMbtiles(inputVecTiles, vt_file),
    mergeMbtiles(inputHillshading, hs_file)]);

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outputfilename);

    const archive = archiver('zip', {
      zlib: { level: 5 } // Sets the compression level.
    });

    output.on('close', resolve);
    archive.on('warning', (err) => { console.warn(err); });
    archive.on('error', (err) => { console.warn(err); reject(err); });
    archive.pipe(output);
    archive.file(hs_file, {name: "hillshading.mbtiles"});
    archive.file(vt_file, {name: "map.mbtiles"});
    archive.finalize();
  });
}

export async function mergeMaps(input: string[], output: string): Promise<void> {
  if (output.match(/zip$/)) {
    return mergeZips(input, output);
  } else {
    return mergeMbtiles(input, output);
  }
}
