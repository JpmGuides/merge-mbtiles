import * as minimist from 'minimist';
import { mergeMaps } from './merge-mbtiles';

const argv = minimist(process.argv.slice(2));

if (!argv.o || argv._.length == 0) {
  console.warn(`Usage: ${process.argv[0]} ${process.argv[1]} -o <output> input1 input2 [input3 ...]`);
  process.exit(1);
}

(async () => {
  try {
    await mergeMaps(argv._, argv.o);
    console.log(argv.o + ': done.');
    process.exit(0);
  } catch (err) {
    console.warn(err);
    process.exit(1);
  }
})();
