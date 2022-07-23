const fetch = require('node-fetch');
const {promises: fs} = require('fs');
const {parse: parseCsv} = require('csv-parse/sync');

const EXISTING_DOCS_FILE = 'documents.json';

/** Top-level function to upload new docs to DocumentCloud. */
async function start() {
  const existingDocs = await loadExistingDocs();
  const existingDocsSet = getExistingDocsSet(existingDocs);

  const profileDocs = await getProfileDocs();

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

  // Trim off the headers in the first row.
  const records = parseCsv(body).slice(1);

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
    }
    if (newDocuments.length === 25) {
      // DocumentCloud takes up to 25 documents at a time.
      break;
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
    data: {'_tag': 'https://github.com/emspishak/nypd-docs'},
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
  const response = await fetch('https://api.www.documentcloud.org/api/documents/', {
    method: 'POST',
    body: JSON.stringify(docs),
    headers: {
      'Authorization': `Bearer ${accessToken}`,
      'Content-Type': 'application/json',
    }});
  if (!response.ok) {
    console.log(`error: ${await response.text()}`);
    return [];
  }
  const data = await response.json();
  if (data.length !== docs.length) {
    console.log(`length mismatch - ${data.length} / ${docs.length}`);
    console.log(`data: ${JSON.stringify(data, null, '\t')}`);
    console.log(`docs: ${JSON.stringify(docs, null, '\t')}`);
    return [];
  }

  const addedDocs = [];
  for (let i = 0; i < data.length; i++) {
    addedDocs.push({
      source_url: docs[i].file_url,
      permanent_url: data[i].canonical_url,
    });
  }
  return addedDocs;
}

/** Writes back out the documents file with the newly updated documents. */
async function writeUpdatedDocs(existingDocs) {
  fs.writeFile(EXISTING_DOCS_FILE, JSON.stringify(existingDocs, null, '\t'));
}

start();
