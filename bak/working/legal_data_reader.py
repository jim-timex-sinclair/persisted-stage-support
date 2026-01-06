# Source - https://stackoverflow.com/q
# Posted by John Vasquez, modified by community. See post 'Timeline' for change history
# Retrieved 2025-12-27, License - CC BY-SA 4.0

from docx import Document
import pandas as pd
import csv 
import json
import time
document = Document('pathtoFile')

tables = []
for table in document.tables:
    df = [['' for i in range(len(table.columns))] for j in range(len(table.rows))]
    for i, row in enumerate(table.rows):
        for j, cell in enumerate(row.cells):
            if cell.text:
                df[i][j] = cell.text
    tables.append(pd.DataFrame(df))

    for nr, i in enumerate(tables):
        i.to_csv("table_" + str(nr) + ".csv")
