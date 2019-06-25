declare module '@mapbox/tilelive';


declare interface Tilesource {
  close(cb: (err?: Error| string) => void): void;
}

declare interface Tilesink {
  close(cb: (err?: Error| string) => void): void;
}

declare interface CopyOptions {
  type: 'scanline' | 'pyramid' | 'list';
  close?: boolean | Function;
}

