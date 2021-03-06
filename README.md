# Asset Allocation Extractor

Asset managers invest their assets in a range of products (equities, fixed income, money markets...). They sometimes report how much of their assets they invest in each class in unstructured annual and quaterly report pdfs. The task was to try to create a script that extracts the allocation values from these pdf reports and puts them into a spreadsheet for later statistical analysis. Several data mining companies were contacted but they did not have a clear solution to the problem. For experimental purposes I was given the task to attempt to come up with a prototype system.

## Getting Started

### Installing
```
git clone https://github.com/gaborcsapo/Asset-Class-Extractor.git
cd Asset-Class-Extractor
pip install -r requirements.txt
```
Once prerequisites are installed, it's ready to go.

### Prerequisites

- java 8
- python 3
- python 2
- C compiler (gcc)

- Note: if tabula-py doesn't install because of an error with numpy, clone the library from github, try editting setup.py by moving numpy from "install-required" to "setup-required" and run "python3 setup.py install"

## Running the program

Once everything is installed, you can run the program using:
```
python3 asset_miner.py 2> /dev/null
```
2> /dev/null surpresses all warnings. The silent argument doesn't work in tabula.read_pdf() and I found this the best solution to filter out the hundreds of Warnings.

Extracts pdfs placed into the ./docs folder.

Final output in results.csv in the main folder. The candidates.csv contains possible results that have been discarded in the process.

Log files are located in ./temp for debugging purposes.

## How it works

### Overview
- takes a pdf and using the library pdf2txt converts each page to text
- filters useless pages by simply throwing them away if they don't contain digits and asset class names
- looks for full sentences and using heuristics tries to guess if they contain information about asset allocation. If it finds a good enough result, skips the next step.
- extracts tables using tabula-py and tries to guess if they contain valueable information
- picks the best result from all potential ones
- converts it to numbers (i.e. 1 billion -> 10000000000) taking currencies into account
- repeats the same for every pdf in ./docs and combines the results in a single file

### Some of the challenges and my response
- The very first problem is the data format, PDF.
	- PDF were meant to create documents that look the same across devices, which created a very open format. It looks the same to human readers, but is a nightmare for computers to understand. The PDF file is really a form of Vector graphic – it contains a whole load of commands to draw shapes, images and text. So long as the end result looks correct that is the key requirement. Often the text will be in the correct order but there is no guarantee. Nothing in PDF specification enforces any standardization. Complex structures such as tables exist because your brain perceives them on the finished document – there is nothing describing them in the PDF beyond a set of draw line commands at certain locations.

	- There are libraries that cluster words and guess the sentence structure with fairly good accuracy in clean cases, but when several unaligned columns of text are on the page, they can get confused. 
	- Table extraction is another level of difficulty. Tables can be made up of pipes separating spaces where the text is placed later. The Tabula open-source project is tackling the problem, but with rather mixed results. Can’t really guess the column structure. It is based on Machine learning, OCR, CV, clustering.

Building a perfect solution already failed.

- Let’s say we extracted the text and table, now we have to understand the data.
	- Extracting numbers from sentences what they are describing are a challenging, but solvable NLP problem. You can either use hundreds of manual rules for all the cases, or experiment with machine learning, once we collected a large training set.
	- Table extraction is a harder one, however. The output of table recognition libraries is completely unreliable, values are misaligned, columns might be broken up into two, and generally the structure of the table cannot be preserved. So I would only consider working with the cleaner cases and discard the rest.

- After solving these problems, we need to find the units. It is a crucial part, as it not negligible if we accidentally report 200 million or billion. In text, it’s usually more straightforward. In tables, while they usually explain in parentheses, from a computer perspective it is very difficult to find where they put those, but again with many manual rules and clustering we can get some good results (given that tables are extracted correctly)

- So far these problems can be overcome if you have a development team and give them 5 months, and in the end you’ll be able to extract the numbers from the PDFs, but you still won’t know what the numbers mean because the context matters immensely:
There are just things computers can’t do and one is understanding human language and how we build context around words. This is really where errors are introduced. (*questions on slide 8*)

More on: https://docs.google.com/a/sociovestix.com/presentation/d/1mKRFsQ53g06lY9MrQ6YKaJdt2BRJ2-K6qE0FfImqXM4/edit?usp=sharing


## Authors

* *Gabor Csapo* - *Initial work* - [Asset Allocation Extractor](https://github.com/Asset-Class-Extractor)


## Acknowledgments

* SocioVestix Labs
* People who created the libraries I use
