"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var util_1 = require("util");
var tilelive = require("@mapbox/tilelive");
var mbtiles_1 = require("@mapbox/mbtiles");
var sqlite3_1 = require("sqlite3");
var archiver = require("archiver");
var unzipper = require("unzipper");
var os = require("os");
var path = require("path");
var fs = require("fs");
var stream = require("stream");
mbtiles_1.registerProtocols(tilelive);
function load(uri) {
    return new Promise(function (resolve, reject) {
        tilelive.load(uri, function (err, result) {
            if (err) {
                reject(err);
            }
            else {
                resolve(result);
            }
        });
    });
}
function loadSource(uri) {
    return load(uri);
}
function loadSink(uri) {
    return load(uri);
}
function copy(src, dest, options) {
    return new Promise(function (resolve, reject) {
        tilelive.copy(src, dest, options, function (err) {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
}
function close(tilestore) {
    return new Promise(function (resolve, reject) {
        tilestore.close(function (err) {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
}
function copyMaps(input, output) {
    return __awaiter(this, void 0, void 0, function () {
        var destDb, runDest, _loop_1, _i, input_1, file;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    destDb = new sqlite3_1.Database(output, sqlite3_1.OPEN_READWRITE);
                    runDest = util_1.promisify(destDb.run.bind(destDb));
                    return [4 /*yield*/, runDest('CREATE TABLE IF NOT EXISTS jpm_map (id NOT NULL UNIQUE, json)')];
                case 1:
                    _a.sent();
                    _loop_1 = function (file) {
                        var sourceDb;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    sourceDb = new sqlite3_1.Database(file, sqlite3_1.OPEN_READWRITE);
                                    return [4 /*yield*/, new Promise(function (resolve, reject) {
                                            sourceDb.each("SELECT id, json from jpm_map", function (err, row) {
                                                return __awaiter(this, void 0, void 0, function () {
                                                    var err_1;
                                                    return __generator(this, function (_a) {
                                                        switch (_a.label) {
                                                            case 0:
                                                                if (err) {
                                                                    return [2 /*return*/, console.warn("Can't read map from " + input + ': ', err)];
                                                                }
                                                                _a.label = 1;
                                                            case 1:
                                                                _a.trys.push([1, 3, , 4]);
                                                                console.log('Adding map for article: ' + row.id);
                                                                return [4 /*yield*/, runDest('INSERT OR REPLACE INTO jpm_map(id, json) VALUES (?, ?)', row.id, row.json)];
                                                            case 2:
                                                                _a.sent();
                                                                return [3 /*break*/, 4];
                                                            case 3:
                                                                err_1 = _a.sent();
                                                                console.warn('Failed to insert map into ' + output + ': ', err_1);
                                                                return [3 /*break*/, 4];
                                                            case 4: return [2 /*return*/];
                                                        }
                                                    });
                                                });
                                            }, function (err, numrows) {
                                                if (err) {
                                                    console.warn("Can't read map from " + input + ': ', err);
                                                }
                                                resolve();
                                            });
                                        })];
                                case 1:
                                    _a.sent();
                                    return [4 /*yield*/, (util_1.promisify(sourceDb.close.bind(sourceDb)))()];
                                case 2:
                                    _a.sent();
                                    return [2 /*return*/];
                            }
                        });
                    };
                    _i = 0, input_1 = input;
                    _a.label = 2;
                case 2:
                    if (!(_i < input_1.length)) return [3 /*break*/, 5];
                    file = input_1[_i];
                    return [5 /*yield**/, _loop_1(file)];
                case 3:
                    _a.sent();
                    _a.label = 4;
                case 4:
                    _i++;
                    return [3 /*break*/, 2];
                case 5:
                    console.log('Cleaning up...');
                    return [4 /*yield*/, runDest('VACUUM')];
                case 6:
                    _a.sent();
                    return [4 /*yield*/, (util_1.promisify(destDb.close.bind(destDb)))()];
                case 7:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function mergeMbtiles(input, output) {
    return __awaiter(this, void 0, void 0, function () {
        var sinkUri, sink, _i, input_2, file, uri, source;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    sinkUri = 'mbtiles://' + output;
                    return [4 /*yield*/, loadSink(sinkUri)];
                case 1:
                    sink = _a.sent();
                    _i = 0, input_2 = input;
                    _a.label = 2;
                case 2:
                    if (!(_i < input_2.length)) return [3 /*break*/, 7];
                    file = input_2[_i];
                    uri = 'mbtiles://' + file;
                    console.log('Copying tiles from: ' + uri + '...');
                    return [4 /*yield*/, loadSource(uri)];
                case 3:
                    source = _a.sent();
                    return [4 /*yield*/, copy(source, sink, {
                            type: 'list',
                            close: false
                        })];
                case 4:
                    _a.sent();
                    console.log('Done copying tiles from: ' + uri + '.');
                    return [4 /*yield*/, close(source)];
                case 5:
                    _a.sent();
                    _a.label = 6;
                case 6:
                    _i++;
                    return [3 /*break*/, 2];
                case 7: return [4 /*yield*/, close(sink)];
                case 8:
                    _a.sent();
                    return [4 /*yield*/, copyMaps(input, output)];
                case 9:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function mktmpdir() {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    fs.mkdtemp(path.join(os.tmpdir(), 'mergezip-'), function (err, folder) {
                        if (err) {
                            reject(err);
                        }
                        else {
                            resolve(folder);
                        }
                    });
                })];
        });
    });
}
function randomString() {
    return Math.random().toString(36).substring(2);
}
function unzipFile(compressedFile, destFolder) {
    return __awaiter(this, void 0, void 0, function () {
        var result;
        return __generator(this, function (_a) {
            result = [];
            return [2 /*return*/, new Promise(function (resolve, reject) {
                    fs.createReadStream(compressedFile)
                        .pipe(unzipper.Parse())
                        .pipe(new stream.Transform({
                        objectMode: true,
                        transform: function (entry, e, cb) {
                            var fileName = entry.path;
                            var type = entry.type; // 'Directory' or 'File'
                            var size = entry.vars.uncompressedSize; // There is also compressedSize;
                            if (fileName === "map.mbtiles" || fileName == 'hillshading.mbtiles') {
                                var dest = destFolder + '/' + randomString() + '-' + fileName;
                                result.push(dest);
                                entry.pipe(fs.createWriteStream(dest))
                                    .on('finish', cb)
                                    .on('error', reject);
                            }
                            else {
                                entry.autodrain();
                                cb();
                            }
                        }
                    }))
                        .on('finish', function () { resolve(result); })
                        .on('error', reject);
                })];
        });
    });
}
function mergeZips(input, outputfilename) {
    return __awaiter(this, void 0, void 0, function () {
        var inputVecTiles, inputHillshading, folder, _i, input_3, file, decompressed, _a, decompressed_1, d, vt_file, hs_file;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    inputVecTiles = [];
                    inputHillshading = [];
                    return [4 /*yield*/, mktmpdir()];
                case 1:
                    folder = _b.sent();
                    _i = 0, input_3 = input;
                    _b.label = 2;
                case 2:
                    if (!(_i < input_3.length)) return [3 /*break*/, 5];
                    file = input_3[_i];
                    return [4 /*yield*/, unzipFile(file, folder)];
                case 3:
                    decompressed = _b.sent();
                    for (_a = 0, decompressed_1 = decompressed; _a < decompressed_1.length; _a++) {
                        d = decompressed_1[_a];
                        if (d.match(/map.mbtiles$/)) {
                            inputVecTiles.push(d);
                        }
                        else if (d.match(/hillshading.mbtiles/)) {
                            inputHillshading.push(d);
                        }
                        else {
                            throw new Error('Unknown file: ' + d);
                        }
                    }
                    _b.label = 4;
                case 4:
                    _i++;
                    return [3 /*break*/, 2];
                case 5:
                    vt_file = folder + '/map.mbtiles';
                    hs_file = folder + '/hillshading.mbtiles';
                    return [4 /*yield*/, Promise.all([
                            mergeMbtiles(inputVecTiles, vt_file),
                            mergeMbtiles(inputHillshading, hs_file)
                        ])];
                case 6:
                    _b.sent();
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            var output = fs.createWriteStream(outputfilename);
                            var archive = archiver('zip', {
                                zlib: { level: 5 } // Sets the compression level.
                            });
                            output.on('close', resolve);
                            archive.on('warning', function (err) { console.warn(err); });
                            archive.on('error', function (err) { console.warn(err); reject(err); });
                            archive.pipe(output);
                            archive.file(hs_file, { name: "hillshading.mbtiles" });
                            archive.file(vt_file, { name: "map.mbtiles" });
                            archive.finalize();
                        })];
            }
        });
    });
}
function mergeMaps(input, output) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            if (output.match(/zip$/)) {
                return [2 /*return*/, mergeZips(input, output)];
            }
            else {
                return [2 /*return*/, mergeMbtiles(input, output)];
            }
            return [2 /*return*/];
        });
    });
}
exports.mergeMaps = mergeMaps;
