const cheerio = require('cheerio');
const fetch = require('node-fetch');
const pdftk = require('node-pdftk');
const {promises: fs} = require('fs');
const {parse: parseCsv} = require('csv-parse/sync');

const EXISTING_DOCS_FILE = 'documents.json';
const DOCS_PER_REQUEST = 25;
const NYC_GOV = 'https://www1.nyc.gov';

/**
 * Documents that exist, but aren't linked to anywhere, which could come (for
 * example) from of a mistake in CCRB's reports.
 */
const EXTRA_DOCS = [
  // Page 4 of https://www.nyc.gov/assets/ccrb/downloads/pdf/prosecution_pdf/apu_quarterly_reports/APUReport2024Q3.pdf
  // incorrecly links to https://www.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/201910097-Tax938534-APU-Final-Documents.pdf
  // where this link should be.
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/201910097-Tax957907-APU-Final-Documents.pdf',
  // Page 15 of https://www.nyc.gov/assets/ccrb/downloads/pdf/prosecution_pdf/apu_quarterly_reports/APUReport2024Q3.pdf
  // incorrectly links to https://www.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202002669-Tax961613-APU-Final-Documents.pdf
  // where this link should be.
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202002792-Tax961613-APU-Final-Documents.pdf',
  // Page 22 of https://www.nyc.gov/assets/ccrb/downloads/pdf/prosecution_pdf/apu_quarterly_reports/APUReport2024Q4.pdf
  // incorrectly links to https://www.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202306511-Tax968628%20-APU-Final-Documents.pdf
  // where this link should be.
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202306511-Tax968628-APU-Final-Documents.pdf',
  // Page 20 of https://www.nyc.gov/assets/ccrb/downloads/pdf/prosecution_pdf/apu_quarterly_reports/APUReport2024Q4.pdf
  // incorrectly links to https://www.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202304412-Tax963837-APU-Final-Documents.pdf
  // where this link should be.
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202304412-Tax967059-APU-Final-Documents.pdf',
  // Page 20 of https://www.nyc.gov/assets/ccrb/downloads/pdf/prosecution_pdf/apu_quarterly_reports/APUReport2024Q4.pdf
  // incorrectly links to https://www.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/2202304127-Tax963837-APU-Final-Documents.pdf
  // where this link should be.
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202304127-Tax963837-APU-Final-Documents.pdf',
];

const DOCS_TO_SKIP = new Set([
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202306511-Tax968628%20-APU-Final-Documents.pdf',
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/202304412-Tax963837-APU-Final-Documents.pdf',
  'https://www1.nyc.gov/assets/ccrb/downloads/pdf/APU-Documents/2202304127-Tax963837-APU-Final-Documents.pdf',
]);

/** Set to true to skip uploading to DocumentCloud. */
const DRY_RUN = false;

/** Top-level function to upload new docs to DocumentCloud. */
async function start() {
  let authToken = null;
  if (!DRY_RUN) {
    authToken = await getAuthToken();
    const failedDocs = await checkDocuments(authToken);
    if (failedDocs !== 0) {
      process.exitCode = 58;
    }
  }

  const existingDocs = await loadExistingDocs();
  const existingDocsSet = getExistingDocsSet(existingDocs);

  const profileDocs = await getProfileDocs();
  // const profileDocs = await getAllProfileDocs();
  // const profileDocs = await getDocsFromFile('some_file.txt');

  const apuDocs = await getApuDocs();

  const apuLinkedDocs = await getApuLinkedDocs(apuDocs, existingDocsSet);

  const ccrbAnnualReports = await getCCRBAnnualReports();

  const departureLetters = await getDepartureLetters();

  const trialDecisions = await getTrialDecisions();

  const ccrbClosingReports = await getCcrbClosingReports();

  const allDocs = [].concat(
      profileDocs,
      apuDocs,
      apuLinkedDocs,
      ccrbAnnualReports,
      departureLetters,
      trialDecisions,
      ccrbClosingReports,
      EXTRA_DOCS,
  ).map((url) => url.replaceAll(' ', '%20'));

  const newDocs = await processDocs(allDocs, existingDocsSet, authToken);
  existingDocs.documents = existingDocs.documents.concat(newDocs);
  console.log(`uploaded ${newDocs.length} docs`);

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
      console.log(
          `::warning ::error fetching ${url}: ${await response.text()}`);
      process.exitCode = 62;
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
  const docsSet = new Set(existingDocs.documents.map((e) => e.source_url));
  existingDocs.documents.flatMap((e) => e.alternate_urls).forEach((e) => docsSet.add(e));
  return docsSet;
}

/** Get the URLs of the NYPD profile documents. */
async function getProfileDocs() {
  const docs = [];
  for (const letter of [...'ABCDEFGHIJKLMNOPQRSTUVWXYZ']) {
    const response = await fetch(
      `https://raw.githubusercontent.com/ryanwatkins/nypd-officer-profiles/refs/heads/main/nypd-profiles-${letter}.json`
    );
    const json = await response.json();
    docs.push(
      ...json.flatMap((officer) =>
        officer.reports.documents?.map((document) => document.url) ?? []
      )
    );
  }
  checkDocCount('profile', 800, docs);
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
  const response = await fetch('https://raw.githubusercontent.com/ryanwatkins/ccrb-complaint-records/refs/heads/main/departureletters.json');
  const json = await response.json();
  const docs = json.departureLetters.map((obj) => obj.FileLink);
  checkDocCount('departure letter', 225, docs);
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
  checkDocCount('trial decision', 1650, docs);
  return docs;
}

/** Gets the URLs of closing reports posted to the CCRB website. */
async function getCcrbClosingReports() {
  const filenames = await getDocsFromCsv(
      'https://www.nyc.gov/assets/ccrb/csv/closing-reports/redacted-closing-reports.csv', 2);
  const docs = filenames.map(
      (filename) => `https://www1.nyc.gov/assets/ccrb/downloads/pdf/closing-reports/${filename}`);
  checkDocCount('CCRB closing report', 3150, docs);
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
  const docs = await getPdfsFromUrl(
      `${NYC_GOV}/site/ccrb/prosecution/apu-quarterly-reports.page`);
  checkDocCount('APU', 25, docs);
  return docs;
}

async function getPdfsFromUrl(url) {
  const response = await fetch(url);
  const html = await response.text();
  const $ = cheerio.load(html);
  const pdfs = $('a')
      .map((i, a) => $(a))
      .filter((i, a) => a.attr('href').endsWith('.pdf'))
      .map((i, a) => NYC_GOV + a.attr('href'));
  return pdfs.get();
}

/** Returns URLs from docs linked to in CCRB APU summary docs. */
async function getApuLinkedDocs(apuDocs, existingDocs) {
  const docs = [];

  for (const apuDoc of apuDocs) {
    // TODO: only run this over new APU docs, once we've scraped links from the
    // existing ones.
    // if (existingDocs.has(apuDoc)) {
    //   continue;
    // }

    // Grab any PDFs linked from the APU reports.
    const response = await fetch(apuDoc);
    const apu = Buffer.from(await response.arrayBuffer());
    // From https://stackoverflow.com/a/43810795
    const out = await pdftk.input(apu).cat().uncompress().output();
    const urls = out.toString().match(/https?:\/\/.+\.pdf/g);
    if (urls !== null) {
      const matches = new Set(
          out.toString()
              .match(/https?:\/\/.+\.pdf/g)
              // Normalize URLs to www1 to avoid duplicates, as this is the format the rest of the CCRB web site uses.
              .map(url => url.replace('https://www.nyc.gov/assets/ccrb/downloads/pdf/', 'https://www1.nyc.gov/assets/ccrb/downloads/pdf/')));
      docs.push(...matches);
    }
  }

  checkDocCount('APU linked', 0, docs);
  return docs;
}

async function getCCRBAnnualReports() {
  const docs = await getPdfsFromUrl(
      `${NYC_GOV}/site/ccrb/policy/annual-bi-annual-reports.page`);
  checkDocCount('CCRB annual reports', 50, docs);
  return docs;
}

/**
 * Check that we have roughly the right number of docs, as a proxy for if
 * scraping is working. Log a warning and set a non-zero error code if there is
 * an issue.
 */
function checkDocCount(docType, expected, docs) {
  console.log(`found ${docs.length} ${docType} docs`);
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
    if (!existingDocs.has(url) && !DOCS_TO_SKIP.has(url)) {
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
    let data = null;
    if (!DRY_RUN) {
      const response =
          await fetch('https://api.www.documentcloud.org/api/documents/', {
            method: 'POST',
            body: JSON.stringify(requestDocs),
            headers: {
              'Authorization': `Bearer ${accessToken}`,
              'Content-Type': 'application/json',
            }});
      if (!response.ok) {
        console.log(
            `::warning ::error: ${await response.text()} ` +
            `on ${JSON.stringify(requestDocs)}`);
        process.exitCode = 60;
        return addedDocs;
      }
      data = await response.json();
      if (data.length !== requestDocs.length) {
        console.log(
            `::warning ::length mismatch - ` +
            `${data.length} / ${requestDocs.length}`);
        console.log(`::warning ::data: ${JSON.stringify(data, null, '\t')}`);
        console.log(
            `::warning ::dequesetDcs: ` +
            `${JSON.stringify(requestDocs, null, '\t')}`);
        process.exitCode = 61;
        return addedDocs;
      }
    }

    for (let i = 0; i < requestDocs.length; i++) {
      addedDocs.push({
        source_url: requestDocs[i].file_url,
        permanent_url: data === null ?
            'DRY_RUN_PLACEHOLDER' :
            data[i].canonical_url,
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
