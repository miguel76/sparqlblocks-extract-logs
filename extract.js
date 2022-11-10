import { readFile, readdir, writeFile } from 'node:fs/promises';

// const EVENT_BEGIN = /^\S* [INFO] client-event \{/mg;
const EVENT_BEGIN = /^\S* \[INFO\] client-event \{/mg;
const EVENT_END = /^\}/mg;

async function* extract(filename) {
  const rawContent = await readFile(filename, { encoding: 'utf8' });
  const contentAsJson = '[' +
      rawContent.replaceAll(EVENT_BEGIN, '{').replaceAll(EVENT_END, '},').slice(0, -2) +
      ']';
  for (const event of JSON.parse(contentAsJson)) {
    yield transform(event);
  };
}

function transform({event: {timestamp: epoch, ...eventData}, timestamp}) {
  return {
    ...eventData,
    timestamp,
    epoch
  }
}

async function* extractFromDir(path) {
  const files = await readdir(path);
  for (const file of files) {
    yield* extract(path + file);
  }
}

async function asJsonCollection(eventAsyncIterable) {
  const jsonCollection = [];
  for await (const event of eventAsyncIterable) {
    jsonCollection.push(event);
  }
  return jsonCollection;
}

async function* serializeAsJsonCollection(eventAsyncIterable) {
  yield '[';
  let first = true;
  for await (const event of eventAsyncIterable) {
    if (first) {
      first = false;
    } else {
      yield ',';
    }
    yield JSON.stringify(event);
  }
  yield ']';
}

async function writeAsJsonCollection(eventAsyncIterable, outputFilename) {
  const stringAsyncIterable = serializeAsJsonCollection(eventAsyncIterable);
  await writeFile(outputFilename, stringAsyncIterable);
}


const filename = '/Users/miguel/Dropbox/SparqlBlocks-eval/logs/' +
    'sparqlblocks-logs/d61a543d-0360-4289-a6f5-bb18e4d112bf/' +
    '000002';

const path = '/Users/miguel/Dropbox/SparqlBlocks-eval/logs/' +
    'sparqlblocks-logs/d61a543d-0360-4289-a6f5-bb18e4d112bf/';

const outputFilename = '/Users/miguel/Dropbox/SparqlBlocks-eval/logs/' +
    'sparqlblocks-logs/pack1.json';


async function etl(path) {
  await writeAsJsonCollection(extractFromDir(path), outputFilename);
}

etl(path).then(
  (result) => {
    // console.log(result);
  },
  (error) => {
    throw new Error(error);
  }
);