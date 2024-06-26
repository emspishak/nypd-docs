const {promises: fs} = require('fs');

/**
 * Adds URLs with new host (without "oip") as "alternate URLs". This is necessary after they redid
 * the NYPD profile page and now all the documents are served from nypdonline.org instead of
 * oip.nypdonline.org.
 */

async function run() {
  const existingDocs = JSON.parse(await fs.readFile('documents.json'));
  for (const doc of existingDocs.documents) {
    if (doc.source_url.match(/^https:\/\/oip.nypdonline.org\/files\/[^/]+\.pdf$/)) {
      doc.alternate_urls = [doc.source_url.replace('https://oip.nypdonline.org/', 'https://nypdonline.org/')];
    }
  }
  fs.writeFile('documents.json', JSON.stringify(existingDocs, null, '\t'));
}

run();
