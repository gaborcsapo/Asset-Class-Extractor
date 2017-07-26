# Asset Allocation Extractor

Asset managers invest their assets in a range of products (equities, fixed income, money markets...). They sometimes report how much of their assets they invest in each class in unstructured annual and quaterly report pdfs. The task was to try to create a script that extracts the allocation values from these pdf reports and puts them into a spreadsheet for later statistical analysis. Several data mining companies were contacted but they did not have a clear solution to the problem. For experimental purposes I was given the task to attempt to come up with a prototype system.

## Getting Started

### Installing
```
git clone https://github.com/gaborcsapo/Asset-Class-Extractor.git
```
Once prerequisites are installed, it's ready to go.

### Prerequisites

- java 8
- python 3
- python 2
- C compiler (gcc)
- Anaconda
- nltk
- tabula-py (go to ./Libraries/tabula-py and run "sudo python3 setup.py install" NOTE: if error encountered, try editting setup.py by moving numpy from "install-required" to "setup-required")
- pdf2txt (go to ./Libraries/pdf2txt and run "sudo python2 setup.py install" NOTE: it's python2 and not 3)
- PyPDF2 (go to ./Libraries/PyPDF2 and run "sudo python3 setup.py install")
- tqdm (go to ./Libraries/tqdm and run "sudo python3 setup.py install")


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
 - Text extraction from PDFs is a notoriously difficult task admitted by many scholars. Table extraction is, however, on another level of complexity because of the unstructured nature of pdfs.
 	- I experimented with three different text extraction libraries, measured their running time and evaluated their results. Pdf2txt has the best results for text extraction with adjustable speed with an accuracy trade-off. It only runs in Python2, however, therefore I make a call outside the main program. PyPDF2, however, can decrypt (not all algorithms, though) and read metadata. 
 	- Didn't really have many options in terms of table extraction. I'm using a python wrapper for a java program called tabula. It is the most widely used program for this task.
 	- I utilize every CPU core through multiprocessing to speed up the process.
 - Tables can be extracted with a large error rate, but even then values are misaligned, columns might be broken up into two, and generally the structure of the table cannot be preserved. 
 	- Unfortunately because of all the ambiguity, I can only consider the small percentage of clean cases. This results in low recall but high accuracy, which is more important as we can have results from thousands of pdfs.
 - Text extraction isn't perfect either. Many times spaces are not recognized between words, words are in the wrong location, and generally the algorithms don't know where to put diagrams, labels, text in tables, so they might just mix it into the main text.
 	- I'm trying to be independent from the context around the information, and mainly look for keywords and see their order. Obviously it comes with a certain error rate.
 - There is almost an infinite number of combinations how one can phrase their asset allocation in sentence. Clauses, percentage values, synonyms, currencies, units(billions, millions) all pose a challenge. 
 	- My algorithm qualifies a number of results and narrows it with each step in it. I'm only concerned about the most common cases, and in situation where ambiguity can be introduced, I simply discard the potential results. 

 It is a Unique Problem with Unique challenges, which make it unfit for out-of-the-box solutions.

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
