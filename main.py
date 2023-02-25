#! /usr/bin/python3

import click
import wget
import gzip

# Improvements notes
# 1. To give a choice to select crawl paths I would download table from https://data.commoncrawl.org/crawl-data/index.html
# 2. Consequently give a choice about the data type WET, WARC, WAT or other files available
# 3. Download path.gz based on choice and extract
# 4. Load the paths to list and give a choice or how many to start extracting or start all
# 5. Concurrency is an issue here as I don't want to bomb my disk with data, thread based solution shall do 
#    - library must exist for this

# Set prefix for getting data from commoncrawl repository through wget
url_prefix = "https://data.commoncrawl.org/"
# prepare list for storing data file names
data = []
# extracting data from gz files 
data_extract = []

with open('wat.paths') as f:
    data_links = [line.rstrip('\n') for line in f]

@click.command()
@click.option("--amount", prompt="Enter amount of data streams to download from")
def get_data(amount):
    click.echo(f"Getting {amount} of data streams!")
    data = [None] * int(amount)
    data_extract = [None] * int(amount)
    for i in range(int(amount)):
        data[i] = wget.download(url_prefix + data_links[i])
        with gzip.open(data[i], 'rb') as f:
            # from here I wanted convert to JSON and search each instance 
            # for Envelope -> Payload-Metadata -> HTML-Metadata -> Links with "path" : "IMG@/src"
            # Take the links, ping them and save the ones that are active
            data_extract[i] = f.read()


if __name__ == "__main__":
    get_data()