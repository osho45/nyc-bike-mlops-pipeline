"""Optional read-only inspection of official public archive names."""
import urllib.request,xml.etree.ElementTree as ET

def main():
    for prefix in ['202401','202402','202501','202503']:
     with urllib.request.urlopen('https://s3.amazonaws.com/tripdata?prefix='+prefix+'&max-keys=8',timeout=20) as r:
      root=ET.fromstring(r.read())
      for c in root:
       if c.tag.split('}')[-1]=='Contents':print({x.tag.split('}')[-1]:x.text for x in c if x.tag.split('}')[-1] in ('Key','Size')})

if __name__=="__main__":main()
