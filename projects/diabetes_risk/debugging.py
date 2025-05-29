# temp file - trying to understand env issues

with open(".env", "rb") as f:
    content = f.read()
    print(content[:4])  # Show the first few bytes


with open(".env", "rb") as f:
    for line in f:
        print(repr(line))

with open(".env") as f:
    print(f.read())

from dotenv import load_dotenv
import os
load_dotenv()
print(os.environ['SNOWFLAKE_USER'])