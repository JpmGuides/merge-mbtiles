
import { promisify } from 'util';
import * as tilelive from "@mapbox/tilelive";
import { registerProtocols } from '@mapbox/mbtiles';
import { Database, OPEN_READWRITE } from 'sqlite3';

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

  await runDest('CREATE TABLE IF NOT EXISTS jpm_map (id, json)');

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
            await runDest('INSERT INTO jpm_map(id, json) VALUES (?, ?)',
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


export async function mergeMbtiles(input: string[], output: string): Promise<void> {
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
