const fetch = require('node-fetch');
const {promises: fs} = require('fs');
const {parse: parseCsv} = require('csv-parse/sync');

const EXISTING_DOCS_FILE = 'documents.json';
const DOCS_PER_REQUEST = 25;

/** Top-level function to upload new docs to DocumentCloud. */
async function start() {
  const existingDocs = await loadExistingDocs();
  const existingDocsSet = getExistingDocsSet(existingDocs);

  const profileDocs = await getProfileDocs();
  // const profileDocs = await getAllProfileDocs();
  // const profileDocs = await getDocsFromFile('some_file.txt');

  const newDocs = await processDocs(profileDocs, existingDocsSet);
  existingDocs.documents = existingDocs.documents.concat(newDocs);

  writeUpdatedDocs(existingDocs);
}

/** Load documents that have already been uploaded. */
async function loadExistingDocs() {
  return JSON.parse(await fs.readFile(EXISTING_DOCS_FILE));
}

/** Get a Set of the source URLs of the already uploaded documents. */
function getExistingDocsSet(existingDocs) {
  return new Set(existingDocs.documents.map((e) => e.source_url));
}

/** Get the URLs of the NYPD profile documents. */
async function getProfileDocs() {
  const response = await fetch(
      'https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/documents.csv');
  const body = await response.text();
  return getDocsFromCsv(body);
}

/**
 * Search the full commit history to find any previous docs that are now
 * removed.
 */
async function getAllProfileDocs() { // eslint-disable-line no-unused-vars
  const docs = new Set();
  let apiUrl = 'https://api.github.com/repos/ryanwatkins/nypd-officer-profiles/commits?path=/documents.csv';
  while (apiUrl) {
    const response = await fetch(apiUrl);
    const json = await response.json();
    for (const commit of json) {
      const hash = commit.sha;
      const documentsResponse = await fetch(
          `https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/${hash}/documents.csv`);
      const body = await documentsResponse.text();
      getDocsFromCsv(body).forEach(docs.add, docs);
    }
    const linkHeader = response.headers.get('link');

    apiUrl = linkHeader.match(/<([^>]+)>; rel="next"/)?.[1];
  }
  // Remove a doc with a malformed URL.
  docs.delete('https://oip.nypdonline.orghttps://oip-admin.nypdonline.org/files/Monjaras_10262021.pdf');
  return Array.from(docs);
}

/** Uploads all docs in a file (each URL on a new line). */
async function getDocsFromFile(filename) { // eslint-disable-line no-unused-vars
  const contents = await fs.readFile(filename);
  return contents.toString().trim().split('\n');
}

/** Takes a CSV string and returns an array of the document URLs. */
function getDocsFromCsv(csvString) {
  // Trim off the headers in the first row.
  const records = parseCsv(csvString).slice(1);

  const docUrls = [];
  for (const doc of records) {
    docUrls.push(doc[2]);
  }
  return docUrls;
}

/** Uploads any new documents and returns the newly uplodaded documents. */
async function processDocs(docUrls, existingDocs) {
  const newDocuments = [];
  for (const url of docUrls) {
    if (!existingDocs.has(url)) {
      newDocuments.push(createDocument(url));
      existingDocs.add(url);
    }
  }
  const token = await getAuthToken();

  return uploadDocs(newDocuments, token);
}

/** Create a Document JSON object for the DocumentCloud API. */
function createDocument(sourceUrl) {
  const urlParts = sourceUrl.split('/');
  return {
    access: 'public',
    data: {'_tag': [
      'https://github.com/emspishak/nypd-docs',
      'NYPD',
    ]},
    file_url: sourceUrl,
    source: sourceUrl,
    title: urlParts[urlParts.length - 1],
  };
}

/** Gets a DocumentCloud auth token. */
async function getAuthToken() {
  const params = new URLSearchParams();
  params.append('username', process.env.DOCUMENT_CLOUD_USERNAME);
  params.append('password', process.env.DOCUMENT_CLOUD_PASSWORD);

  const response = await fetch('https://accounts.muckrock.com/api/token/', {
    body: params,
    method: 'POST',
  });
  const data = await response.json();
  return data.access;
}

/**
 * Uploads the given docs (in DocumentCloud JSON format) and returns details
 * about the updated docs.
 */
async function uploadDocs(docs, accessToken) {
  const addedDocs = [];
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < Math.ceil(docs.length / DOCS_PER_REQUEST); i++) {
    if (i !== 0) {
      // DocumentCloud allows 10 requests per second, so wait 100ms between
      // requests.
      await delay(100);
    }

    const requestDocs =
        docs.slice(i * DOCS_PER_REQUEST, (i + 1) * DOCS_PER_REQUEST);
    const response =
        await fetch('https://api.www.documentcloud.org/api/documents/', {
          method: 'POST',
          body: JSON.stringify(requestDocs),
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json',
          }});
    if (!response.ok) {
      console.log(`error: ${await response.text()}`);
      return addedDocs;
    }
    const data = await response.json();
    if (data.length !== requestDocs.length) {
      console.log(`length mismatch - ${data.length} / ${requestDocs.length}`);
      console.log(`data: ${JSON.stringify(data, null, '\t')}`);
      console.log(`dequesetDcs: ${JSON.stringify(requestDocs, null, '\t')}`);
      return addedDocs;
    }

    for (let i = 0; i < data.length; i++) {
      addedDocs.push({
        source_url: requestDocs[i].file_url,
        permanent_url: data[i].canonical_url,
      });
    }
  }
  return addedDocs;
}

/** Writes back out the documents file with the newly updated documents. */
async function writeUpdatedDocs(existingDocs) {
  fs.writeFile(EXISTING_DOCS_FILE, JSON.stringify(existingDocs, null, '\t'));
}

start();
