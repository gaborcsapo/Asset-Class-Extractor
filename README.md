# Installation:

You need to have:
- java8
- python3
- python2
- C compiler (gcc)
- Anaconda
- nltk
- tabula-py (go to ./Libraries/tabula-py and run "sudo python3 setup.py install" NOTE: if error encountered, try editting setup.py by moving numpy from "install-required" to "setup-required")
- pdf2txt (go to ./Libraries/pdf2txt and run "sudo python2 setup.py install" NOTE: it's python2 and not 3)
- PyPDF2 (go to ./Libraries/PyPDF2 and run "sudo python3 setup.py install")
- tqdm (go to ./Libraries/tqdm and run "sudo python3 setup.py install")

Once everything is installed, you can run the program using "python3 asset_miner.py 2> /dev/null". "2> /dev/null" surpresses all warnings including the ones that can from tabula. The silent argument doesn't work in tabula.read_pdf() and I found this the best solution to filter out the Warnings.

It extracts pdfs placed into the ./docs folder.

results.csv in the main folder containes the final output. The candidates.csv contains possible results that have been discarded in the process.

If you go to ./temp, you can read the logging files for each child process after they are run. It contains useful information for debugging.

# The problem:
Asset managers invest their assets in a range of products (equities, fixed income, money markets...). They sometimes report how much of their assets they invest in each class in unstructured annual and quaterly report pdfs. The task was to try to create a script that extracts the allocation values from these pdf reports and puts it into a spreadsheet for later statistical analysis. Several data mining companies were contacted but they did not have a clear solution to the problem. For experimental purposes I was given the task to attempt to come up with a prototype system.

# How it works:
- takes a pdf and using the library pdf2txt converts each page to text
- filters useless pages by throwing them away simply if they don't contain digits and asset class names
- looks for full sentences and using heuristics tries to guess if they contain information about asset allocation. If it finds a good enough result, skips the next step.
- extracts tables and tries to guess if they contain valueable information
- picks the best result from all potential ones
- converts it to numbers (i.e. 1 billion -> 10000000000)
- repeats the same for every pdf in ./docs and combines the results in a single file

## Challenges and my response:
 - Text extraction from PDFs is a notoriously difficult task admitted by many scholars. Table extraction is, however, on another level of complexity because of the unstructured nature of pdfs.
 	- I experimented with three different ttext extraction libraries, measured their running time and evaluated their results. Pdf2txt has the best results with adjustable speed with an accuracy trade-off. It only runs in Python2, however, therefore I make a call outside the main program.
 	- Didn't really have many options in tterms of table extraction. I'm using a python wrapper for a java program called tabula. It is the most widely used program for this task.
 	- I utilize every CPU core through multiprocessing to speed up the process.
 - Tables can be extracted with a large error rate, but even then values are misaligned, columns might be broken up into two, and generally the structure of the table cannot be preserved. 
 	- Unfortunately because of all the ambiguity, I only consider clean cases. This results in low recall but high accuracy, which is more important as we can have results from thousands of pdfs.
 - Text extraction isn't perfect either. Many times spaces are not recognized between words, words are in the wrong location, and generally the algorithms don't know where to put diagrams, labels, text in tables, so they might just mix it into the main text.
 	- I'm trying to be independent from the context around the information, and mainly look for key words and see their order. Obviously there is a certain error rate.
 - There is almost an infinite number of combinations how one can phrase their asset allocation in sentence. Clauses, percentage values, synonyms, currencies, units(billions, millions) all pose a challenge. 
 	- My algorithm qualifies a number of results and narrows it with each step in it. I'm only concerned about the most common cases, and in situation where ambiguity can be introduced, I simply discard the potential results. 





Results from example pdfs:


#sentence
Clients invest across the full product range, as evidenced by the year-end mix of 45% in equities, 28% in fixed income, 4% in multi-asset class, 3% in alternatives, 13% in cash management and 7% in advisory mandates. 

The product mix is well diversified, with 40%, or $153.9 billion, invested in  equities, 24%, or $93.2 billion, in fixed income, 17% in each of multi-asset class ($65.2 billion) and cash management ($67.6 billion), and 2%, or $8.9 billion, in alternative investment products.The client base is also diversified geographically, with $213.8 billion, or 67%, of long-dated AUM managed for retail and high net worth investors based in the United States and Canada, $71.5 billion, or 22%, for investors based in EMEA, $29.6 billion, or 9%, in Asia Pacific, and $6.3 billion, or 2%, in Latin America and Iberia.


#chart Work on 37. New York sth table extraction

#chart Templeton Investments works

#sentence Blackrock
Clients invest across the full
BlackRock serves clients in more than 100 countries product range, as evidenced by the year-end mix of 45%
through the efforts of professionals located in 24 coun- in equities, 28% in fixed income, 4% in multi-asset class,
tries. Our global presence enables us to deliver highly 3% in alternatives, 13% in cash management and 7% in
responsive service and to tailor our offerings to best advisory mandates

#Chart or pairing Rowe Price NO


#stupid chart Allianz won't work

#Northern Trust good chart but doesn't work, no pairing

#chart could work NEEDS AREA TWEEKING J.P morgran

#EncryptionError Goldman Sachs

#chart could work needs area tweeking Invesco pairing could work

#encryptionError DeutscheBank


in € bn
in billions
in millions
($ In Billions)
($ billions)
(dollar amounts in billions)
(in billions)
sometimes they have different charts with different units...