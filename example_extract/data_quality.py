#
if len(df.head(1))==0:
    raise ValueError("Landing quality failed | dataset is empty.")

print("Landing file is not empty")