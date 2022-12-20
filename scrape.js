const cheerio = require('cheerio');
const fetch = require('node-fetch');
const {promises: fs} = require('fs');
const {parse: parseCsv} = require('csv-parse/sync');

const EXISTING_DOCS_FILE = 'documents.json';
const DOCS_PER_REQUEST = 25;
const NYC_GOV = 'https://www1.nyc.gov';

/** Top-level function to upload new docs to DocumentCloud. */
async function start() {
  const authToken = await getAuthToken();
  const failedDocs = await checkDocuments(authToken);
  if (failedDocs !== 0) {
    process.exitCode = failedDocs;
  }

  const existingDocs = await loadExistingDocs();
  const existingDocsSet = getExistingDocsSet(existingDocs);

  const profileDocs = await getProfileDocs();
  // const profileDocs = await getAllProfileDocs();
  // const profileDocs = await getDocsFromFile('some_file.txt');

  const apuDocs = await getApuDocs();

  const departureLetters = await getDepartureLetters();

  const trialDecisions = await getTrialDecisions();

  const ccrbClosingReports = await getCcrbClosingReports();

  const allDocs = [].concat(
      profileDocs,
      apuDocs,
      departureLetters,
      trialDecisions,
      ccrbClosingReports,
  );

  const newDocs = await processDocs(allDocs, existingDocsSet, authToken);
  existingDocs.documents = existingDocs.documents.concat(newDocs);

  writeUpdatedDocs(existingDocs);
}

/**
 * Check that already uploaded docs have successfully processed. This will exit
 * with a non-zero exit code (after uploading all docs) to alert that there are
 * some docs that need manual fixing.
 */
async function checkDocuments(authToken) {
  let badDocCount = 0;

  let url = 'https://api.www.documentcloud.org/api/documents/?format=json&user=106646&per_page=100';
  while (url !== null) {
    const response = await fetch(url, {
      headers: {
        'Authorization': `Bearer ${authToken}`,
      }});
    if (!response.ok) {
      console.log(`error: ${await response.text()}`);
      return -1;
    }

    const data = await response.json();
    for (const doc of data.results) {
      if (doc.status !== 'success') {
        badDocCount++;
        console.log(
            `::warning ::found unprocessed doc: ${JSON.stringify(doc)}`);
      }
    }

    url = data.next;
    if (url !== null) {
      // DocumentCloud allows 10 requests per second, so wait 100ms between
      // requests.
      await delay(100);
    }
  }

  return badDocCount;
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
  const docs = await getDocsFromCsv('https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/documents.csv', 2);
  console.log(`found ${docs.length} profile docs`);
  checkDocCount('profile', 750, docs);
  return docs;
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

/** Get the URLs of NYPD Departure Letters (from the CCRB website). */
async function getDepartureLetters() {
  const docs = await getDocsFromCsv('https://raw.githubusercontent.com/ryanwatkins/ccrb-complaint-records/main/departureletters.csv', 6);
  console.log(`found ${docs.length} departure letter docs`);
  checkDocCount('departure letter', 150, docs);
  return docs;
}

/**
 * Gets the URLs of NYPD Trial Decisions, which are scraped from
 * https://nypdonline.org/link/1016
 */
async function getTrialDecisions() {
  const response = await fetch(
      'https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/main/trial-decisions.json');
  const json = await response.json();
  const docs = json.map((record) => record.url);
  console.log(`found ${docs.length} trial decision docs`);
  checkDocCount('trial decision', 1500, docs);
  return docs;
}

/** Gets the URLs of closing reports posted to the CCRB website. */
async function getCcrbClosingReports() {
  const filenames = await getDocsFromCsv(
      'https://www.nyc.gov/assets/ccrb/csv/closing-reports/redacted-closing-reports.csv', 2);
  const docs = filenames.map(
      (filename) => `https://www1.nyc.gov/assets/ccrb/downloads/pdf/closing-reports/${filename}`);
  console.log(`found ${docs.length} CCRB closing report docs`);
  checkDocCount('CCRB closing report', 1000, docs);
  return docs;
}

/** Uploads all docs in a file (each URL on a new line). */
async function getDocsFromFile(filename) { // eslint-disable-line no-unused-vars
  const contents = await fs.readFile(filename);
  return contents.toString().trim().split('\n');
}

/**
 * Takes a URL of a CSV and returns an array of the document URLs that are in
 * the given column of the CSV.
 */
async function getDocsFromCsv(url, docColumn) {
  const response = await fetch(url);
  const body = await response.text();

  // Trim off the headers in the first row.
  const records = parseCsv(body).slice(1);

  const docUrls = [];
  for (const doc of records) {
    docUrls.push(doc[docColumn]);
  }
  return docUrls;
}

/** Returns URLs of all CCRB APU summary docs. */
async function getApuDocs() {
  const response = await fetch(
      `${NYC_GOV}/site/ccrb/prosecution/apu-quarterly-reports.page`);
  const html = await response.text();
  const $ = cheerio.load(html);
  const pdfs = $('a')
      .map((i, a) => $(a))
      .filter((i, a) => a.attr('href').endsWith('.pdf'))
      .map((i, a) => NYC_GOV + a.attr('href'));
  const docs = pdfs.get();
  console.log(`found ${docs.length} APU docs`);
  checkDocCount('APU', 15, docs);
  return docs;
}

/**
 * Check that we have roughly the right number of docs, as a proxy for if
 * scraping is working. Log a warning and set a non-zero error code if there is
 * an issue.
 */
function checkDocCount(docType, expected, docs) {
  if (docs.length < expected) {
    console.log(
        `::warning ::expected at least ${expected} ${docType} docs, but got ` +
            docs.length);
    process.exitCode = 59;
  }
}

/** Uploads any new documents and returns the newly uploaded documents. */
async function processDocs(docUrls, existingDocs, authToken) {
  const newDocuments = [];
  for (const url of docUrls) {
    if (!existingDocs.has(url)) {
      newDocuments.push(createDocument(url));
      existingDocs.add(url);
    }
  }

  return uploadDocs(newDocuments, authToken);
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

/** Returns a promise that resolves after the given number of milliseconds. */
async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Uploads the given docs (in DocumentCloud JSON format) and returns details
 * about the updated docs.
 */
async function uploadDocs(docs, accessToken) {
  const addedDocs = [];
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
