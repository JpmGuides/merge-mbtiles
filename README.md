# Merge multiple map archives together

This code can be used either as a javascript/typescript library, or as an executable.

## Executable usage


Let's assume we have two map archives: map1.zip and map2.zip. To merge them, we do:

```
node dist/index.js -o merged.zip map1.zip map2.zip
```

The tool can work with .zip or .mbtiles files, but can't work with a mix of both.


## Library

There is a single function exposed in this library:

```
export declare function mergeMaps(input: string[], output: string): Promise<void>;
```

## Testing

```npm test``` will run the tests.
