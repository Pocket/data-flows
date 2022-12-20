Data taken from: https://docs.google.com/spreadsheets/d/1gdl9695Ib6Kd0OUX28yD03wJzRAcHiKZkZqn9DVdI2I/edit#gid=1108537720

Steps to dump this data:
1. Use Snowflake to dump all syndicated items from the Corpus:
    ```sql
    SELECT URL, APPROVED_CORPUS_ITEM_EXTERNAL_ID as CORPUS_ID, TITLE, TOPIC
    FROM "APPROVED_CORPUS_ITEMS"
    WHERE IS_SYNDICATED = TRUE
    ORDER BY URL ASC;
    ```
2. Open the spreadsheet above. In the 'Syndicated Corpus Items' sheet, choose File > Import. Select the Snowflake dump.
3. Go to the 'FINAL PICKS' sheet. Check that the 'ID' column has values for every row.
4. copy the 'ID' column into a text editor / IDE.
5. Convert CSV to Python by doing a regex replace all:
    ```regexp
    Find:
    ^(.*)$
    
    Replace with:
    "$1", 
    ```
