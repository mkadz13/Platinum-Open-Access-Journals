Manually Downloading JIF CSV Files (Clarivate JCR)

Note: The DOAJ CSV is downloaded automatically by GitHub Actions.

1. Go to https://login.proxy1.lib.uwo.ca/login?qurl=https://jcr.clarivate.com%2fJCRJournalHomeAction.action%3f and log in with your Western credentials, if prompted then register a JCR account
2. After logging in, click the "Journals" tab at the top of the page
3. Click Filter, then set JCR year to the most recent year available and apply the filter
4. In the GitHub repo, navigate to data/doaj_issns.txt, you will see ISSNs split into segments (Segment 1, Segment 2, etc...)

Download JCR CSVs (IMPORTANT)
You must do this one segment at a time.
For each segment in doaj_issns.txt:

5. Copy the entire ISSN list for that segment
6. In JCR, open Filter --> ISSN
7. Paste the ISSNs into the ISSN field and click apply
8. In the top-right corner, click export as CSV
9. Repeat for all of the segments

Generate the Combined JIF CSV (on local machine):

10. Open combine_jif.py, locate the year variable (around line 76) and change it to the year you extracted the data from
11. From the project root, run: python combine_jif.py. A file named JIF{year}.csv will be created in the data directory.
12. Commit the new JIF file and push it to GitHub. GitHub Actions will automatically detect the new JIF file, and rebuild the site.
