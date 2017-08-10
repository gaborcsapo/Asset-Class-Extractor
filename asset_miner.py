import subprocess
import re
import tabula
import nltk
from os import getcwd, listdir, getpid, remove, devnull
from os.path import isfile, join
import pandas as pd
from PyPDF2 import PdfFileReader
import csv
from multiprocessing import Pool 
from tqdm import tqdm
import time
import sys

#regexs to distinguish dollar values, asset classes and percent values
re_digits = "(?<!\d)(?:[1-9]\d*|0)(?:[,\.-]\d+| \d{3}){0,3}(?!\d)"
re_currency = "(?:USD|US dollars?|dollars?|EUR|euro|GBP|pounds|CHF|JPY|CNY|CAD|AUD|DKK|BRL|KRW|SEK|[$¢£¤¥֏؋৲৳৻૱௹฿៛\u20AC\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6])"
re_words = "(?:(?:million|thousand|hundred|billion|M|B|T|K|m|bn|tn)(?![a-z]))"

re_context = '(?:mix|management|assets|investment|managed|invested)?'
re_headings = '(?:Fund mix|Assets|Group investments|Investment allocation|Fund management|Investment management|assets|sset class|aum)'
re_classes = '(?:equities|equity|fixed income|alternatives?|multi[ -]assets?|hybrid|cash|cash management|money markets?|bonds?|stocks?|liquidity)'
loose_search = '(?:(equities|equity|stocks?)|(fixed income|bonds?)|(alternatives?)|(multi[ -]assets?|hybrid)|(cash|cash management|money markets?|liquidity))'
re_money = re_currency+'?\s?'+re_digits+'\s?'+re_words+'?\s?'+re_currency+'?(?![%\d])'
re_percent = re_digits+'%'

re_million = '(?:(?:'+re_currency+'[a-z ]{0,10}(in )?)?(mill(ions?)?)([a-z ]{0,6}'+re_currency+')?|'+re_currency+'m)'
re_billion = '(?:(?:'+re_currency+'[a-z ]{0,10}(in )?)?(bn|bill(ions?)?)([a-z ]{0,6}'+re_currency+')?)'
re_trillion = '(?:(?:'+re_currency+'[a-z ]{0,10}(in )?)?(tn|trill(ions?)?)([a-z ]{0,6}'+re_currency+')?)'

HEADER = ['name', 'date', 'file', 'equity', 'equity%', 'fixed income', 'fixed income%', 'alternative', 'alternative%', 'multi-asset','multi-asset%', 'money market', 'money market%']


class Document:
	def __init__(self, file):
		self.path = "./docs/" + file
		self.title = None
		self.date = None
		self.OFFSET = 0
		self.pdf_reader = PdfFileReader(open(self.path, "rb"), strict=False)
		self.page_dict = {}


def main():
	files_pdf = [f for f in listdir('./docs/') if (isfile(join('./docs/', f)) and f.endswith('.pdf'))]
	clean_up('.log')
	clean_up('.csv')
	clean_up('.txt')

	#multi processing. Breking the list of files and feeeding them to a bunch of processes
	pool = Pool()
	for _ in tqdm(pool.imap_unordered(extract, files_pdf), total=len(files_pdf), file=sys.stdout):
		pass
	pool.close() 
	pool.join()
	
	#getting the result of the subprocesses and writing them in one file
	for current in ['results','candidates']:
		with open(current+'.csv', 'w', encoding='utf-8') as file_out:
			writer = csv.writer(file_out, dialect='excel')
			writer.writerow(HEADER)
			for file in [f for f in listdir('./temp') if (isfile(join('./temp', f)) and f.endswith('.csv') and f.startswith(current))]:
				with open('./temp/'+file, 'r', encoding='utf-8') as f:
					for line in f:
						file_out.write(line)

	#clean_up('.csv')
	clean_up('.txt')


def extract(doc_name):
	doc = Document(doc_name)
	#check if pdf is encrypted
	try:
		if (doc.pdf_reader.isEncrypted):
			if (doc.pdf_reader.decrypt('') == 0):
				print('Cannot decrypt file, skipping ', doc_name)
				return
	except Exception as inst:
		print("Decryption error, skipping ", doc_name)
		return

	#global variables for logging purposes
	global LOG_FILE
	global TIMER
	TIMER = time.time()
	LOG_FILE = open('./temp/logging_pid:'+str(getpid())+'.log', 'a', encoding='utf-8')
	
	check_title(doc, doc.path)
	load_doc(doc)
	page_filter(doc)
	
	candidates = []
	candidates += text_extract(doc)
	if (len(candidates) > 0):	
		if (evaluate(candidates).count() > 6):
			pass 
		else:
			candidates += table_extract(doc, doc.page_dict)
	else:
		candidates += table_extract(doc, doc.page_dict) #table_extract(doc, [doc.OFFSET]) #

	if (len(candidates) == 0):
		print('#####Done: No candidates found in', doc.title, file=LOG_FILE)
		LOG_FILE.close()
		return
	best = evaluate(candidates)
	
	adjust_unit(doc, best)
	write_csv([best], True)
	write_csv(candidates, False)
	print('###Extract done### ', time.time()-TIMER, file=LOG_FILE)
	LOG_FILE.close()
	

def check_title(doc, file):
	#checking if name meets standards for later printing use, and 
	title = re.compile("\d{1,3}\. [\S\s]+? \([A-Za-z ]+\)")
	date = re.compile("\d{4} ?Q?[1-4]?")
	title_search = title.search(file[7:])
	date_search = date.search(file[7:])
	if (bool(title_search) and bool(date_search)):
	    doc.title = title_search.group(0)
	    doc.date = date_search.group(0)
	    print("####", doc.title, doc.date, "####", file=LOG_FILE)
	else:
		print(file, " doesn't meet naming conventions", file=LOG_FILE)
		doc.title = 'UNKOWN'
		doc.date = 'UNKOWN'
	#doc.OFFSET = int(file[7:9])


def load_doc(doc):
	print("####CONVERTING PDF TO TEXT####", file=LOG_FILE)
	
	pages = doc.pdf_reader.getNumPages()
	
	# I load the pages into the document object.
	for page in range(pages):
	    doc.page_dict[page] = ''
	    
	    #converting to text using python2 script, which output to ./temp
	    cmd = ["python2", "./Libraries/pdf2txt/tools/pdf2txt.py", "-o", "./temp/out_pid:"+str(getpid())+'_'+str(page)+".txt", "-p", str(page),"-t", "text", doc.path]
	    try:
	        subprocess.check_output(cmd)
	    except subprocess.CalledProcessError as e:
	        print("Text extraction failed in ", doc.title, e.output)
	        break

	    #reading in files to get text from ./temp   
	    with open("./temp/out_pid:"+str(getpid())+'_'+str(page)+".txt", "r", encoding='utf-8') as page_output:
	        for line in page_output:
	            doc.page_dict[page] += line.strip() + ' '
	    page_output.close()

	print("Document loaded", time.time()-TIMER, file=LOG_FILE)


# Once loaded, the document has to go through a basic page scoring step. If the page doesn't contain Asset/Investment allocation or the two words equity and fixed income, then the page is thrown away.
def page_filter(doc):
	page_nomination = []
	
	digit_search = re.compile(re_digits, re.IGNORECASE)
	class_search = re.compile(loose_search, re.IGNORECASE)
	context_search = re.compile(re_headings+'|'+re_context, re.IGNORECASE)
	#we are only interested in pages that more than 3 digits, at least 3 kinds of asset classes and containes equities and fixed income and has a contextual word in it as well
	for page in doc.page_dict:
	    digit_match = 3 < len(digit_search.findall(doc.page_dict[page]))
	    class_match = 2 < len(class_search.findall(doc.page_dict[page]))
	    eq_search = bool(re.search(r'equity|equities',doc.page_dict[page], re.IGNORECASE))
	    fx_search = bool(re.search(r'fixed income',doc.page_dict[page], re.IGNORECASE))
	    context_match = bool(context_search.search(doc.page_dict[page]))
	    if (digit_match and class_match and context_match and eq_search and fx_search):
	        page_nomination.append(page)
	
	#taking subset of original document object
	doc.page_dict = {k: doc.page_dict[k] for k in page_nomination}
	print("Pages to look through:", len(doc.page_dict), time.time()-TIMER, file=LOG_FILE)


# To analyze the pages, I'll try to convert it to a table and then I'll validate the first table column entries (equity, fixed income, and optionally multi asset and alternatives) I'll also try to look for columns indicating years.
def table_extract(doc, pages_to_extract):
	print("####TABLE EXTRACTION####", file=LOG_FILE)
	results = []
	page = doc.pdf_reader.getPage(0)

	#table extraction sometimes works better if we only feed in a small portion of the page
	#this app looks at the whole, upper and lower half and the 4 quaters of the page
	top = page.mediaBox.getUpperRight_y()
	right = page.mediaBox.getUpperRight_x()
	bottom = page.mediaBox.getLowerLeft_y()
	left = page.mediaBox.getLowerLeft_x()
	areas = [
	         [top, left, (bottom+top)/2, right],[(bottom+top)/2, left, bottom, right],
	         [top, left, bottom, right],
	         [top, left, (bottom+top)/2, (right+left)/2],[top, (right+left)/2, (bottom+top)/2, right],
	         [(bottom+top)/2, (right+left)/2, bottom, right],[(bottom+top)/2, left, bottom, (right+left)/2]
	        ]

	k = re.compile(re_classes, re.IGNORECASE)
	j = re.compile(re_money, re.IGNORECASE)
	l = re.compile(re_percent, re.IGNORECASE)
	
	for page in pages_to_extract:
	    for idx, area in enumerate(areas):
	        found = False
	        series = pd.Series(index=HEADER)
	        series['name'] = doc.title
	        series['date'] = doc.date
	        #if first two didn't yield anything, give up
	        if (idx == 2 and not found):
	        	break
	        try:
	            table = tabula.read_pdf(doc.path, pages=page, area = area, silent=True)
	            print(table, file=LOG_FILE)
	            found = True
	            p = table.T.reset_index().T
	            p = p.apply(lambda x: x.astype(str).str.lower())
	            
	            #we traverse the table looking for valid asset classes and values
	            for row_ind, row in p.iterrows():
	                search = True
	                for col_ind, col in row.iteritems():
	                    #first we look for the asset class in the table
	                    if search:
	                    	#if there are several asset classes in a cell, we can't do anthing 
	                        if (len(k.findall(col))>1):
	                            break
	                        if bool(k.search(col)):
	                            asset = k.search(col).group(0)
	                            search = False
	                            #sometimes the value and the class ends up in the same cell
	                            if bool(j.search(col)):
	                                into_series(series, asset, j.search(col).group(0))
	                                break
	                            if bool(l.search(col)):
	                                into_series(series, asset, l.search(col).group(0), '%')
	                                break
	                    #I make the assumption that in the table structure, we'll first find the asset class and the value would follow
	                    else:
	                        if bool(j.search(col)):
	                            into_series(series, asset, j.search(col).group(0))
	                            break
	                        if bool(l.search(col)):
	                            into_series(series, asset, l.search(col).group(0), '%')
	                            break
	            
	            #if than 4 values in the series, it is result. I use 4 because 1 is name and 1 is date, so actually it's more than 2           
	            if (series.count() > 4):
	                series['file'] = getcwd()+doc.path[1:]+'#page='+str(page)
	                results.append(series)
	                print("-------Results += 1 on p", page, file=LOG_FILE)
	                print(series, file=LOG_FILE)
	                print("-------", file=LOG_FILE)
	                print(table, file=LOG_FILE)
	                print("-------", file=LOG_FILE)
	                #I use 6 here because in the meantime file loc was addaed too
	                if (series.count() > 6):
	                    break            
	        except Exception as inst:
	            print("Unsuccessful table search: page", page, inst, file=LOG_FILE)
	    print('P',page,' took ', time.time()-TIMER, 'with ', len(results), 'results', file=LOG_FILE)
	return results


# Here I'm trying to text mine the expected results. This regex looks for conversational sentences.	And based on the position of numbers and asset classes guesses what number corresponds to which asset class.
def text_extract(doc):
	print("####TEXT EXTRACTION####", time.time()-TIMER, file=LOG_FILE)
	
	tokenizer = nltk.tokenize.RegexpTokenizer(r''+re_percent+'|'+re_classes+'|'+re_money)
	regexp_tagger = nltk.tag.RegexpTagger(
	    [(r''+re_classes, 'CL'),
	     (r''+re_money, 'NU'),
	     (r''+re_percent, 'PC'),     
	    ])
	results = []
	
	for page in doc.page_dict:
	    text = doc.page_dict[page]
	    
	    #regex to find full sentences
	    h = re.compile('[A-Z][^\.][\s\S]*?[^A-Z][.?!](?![A-Z][^a-z]|\d)')
	    matches = h.findall(text)
	    
	    #sentence is dropped if sentence has less than 2 contextual keywords and either doesn't have digits or asset class names
	    to_drop = []
	    j = re.compile(re_classes+'|'+re_context, re.IGNORECASE)
	    k = re.compile(re_classes, re.IGNORECASE)
	    
	    for i in range(len(matches)-1, -1, -1):
	        matches[i] = matches[i].replace('\n', ' ').replace('\r', '')
	        length = 0 if bool(j.search(matches[i])) else len(j.findall(matches[i]))
	        if(length < 2 and (not bool(k.search(matches[i])) or not bool(re.search(r'\d', matches[i])))):
	            to_drop.append(i)

	    for i in to_drop:
	        matches.pop(i)
	    

		#tokenize matches into the useful tokens and tag them whether they're nubmers or classes blabla
	    for match in matches:
	        series = pd.Series(index=HEADER)
	        tokens = tokenizer.tokenize(match)
	        tags = regexp_tagger.tag(tokens)
	        
	        #we only use the sentence if the number of asset classes match up with the number values. Otherwise the sentences area just a mess
	        pc = nu = cl = 0
	        for ind, i in enumerate(tags):
	            if i[1] == 'PC':
	                pc += 1
	            elif i[1] == 'NU':
	                nu += 1
	            elif i[1] == 'CL':
	                cl += 1
	            #if at least 3 classes and numbers align only take those and skip the rest
	            if (abs(cl - nu) > 1):
	                if (cl>2):
	                    tags = tags[0:ind-1]
	                    nu = cl
	                break
            #different combinations of pc, num, cla
	        per = num = cla = None
	        if (pc == cl == nu):
	            for i in tags:        
	                if i[1] == 'PC':
	                    if per == None:
	                        per = i[0]
	                    else:
	                        break
	                if i[1] == 'NU':
	                    if num == None:
	                        num = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                    if cla == None:
	                        cla = i[0]
	                    else:
	                        break
	                if per and num and cla:
	                    into_series(series, cla, num)
	                    into_series(series, cla, per, '%')
	                    per = num = cla = None
	                    
	        elif (cl == pc):
	            for i in tags:        
	                if i[1] == 'PC':
	                    if per == None:
	                        per = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                   if cla == None:
	                       cla = i[0]
	                   else:
	                       break
	                if per and cla:
	                   into_series(series, cla, per, '%')
	                   cla = per = None
	        elif (cl == nu):
	            for i in tags:        
	                if i[1] == 'NU':
	                    if num == None:
	                       num = i[0]
	                    else:
	                        break
	                if i[1] == 'CL':
	                    if cla == None:
	                        cla = i[0]
	                    else:
	                        break
	                if num and cla:
	                    into_series(series, cla, num)
	                    num = cla = None
	        
	        #putting qualifying results into candidates
	        if (series.count() > 3):
	            series['file'] = getcwd()+doc.path+'#page='+str(page)
	            series['name'] = doc.title
	            series['date'] = doc.date
	            results.append(series)
	            print("-------Results += 1 on p", page, file=LOG_FILE)
	            print(match, file=LOG_FILE)
	            print("-------", file=LOG_FILE)
	            print(series, file=LOG_FILE)
	            print("-------", file=LOG_FILE)
	            print(tokens, file=LOG_FILE)
	            print("-------", file=LOG_FILE)
	            print(tags, file=LOG_FILE)
	            print("-------", file=LOG_FILE)
	            if (series.count() > 5):
	            	break                      
	print('Done with text. ', time.time()-TIMER, 'with ', len(results), 'results', file=LOG_FILE)
	return results


def adjust_unit(doc, best):
	match = re.search('page=(\d+)', best['file'])
	page = int(match.group(1)) if match else 0
	
	for ind, value in best[['equity','fixed income','alternative','multi-asset','money market']].iteritems():
		number = 1
		if isinstance(value, str):
			#looking through the pages
			unit_search = [re.findall(re_trillion, doc.page_dict[page], re.IGNORECASE),
							re.findall(re_billion, doc.page_dict[page], re.IGNORECASE),
							re.findall(re_million, doc.page_dict[page], re.IGNORECASE)]
			most_freq_unit = max(enumerate(unit_search), key = lambda tup: len(tup[1]))
			#looking through the value
			if (bool(re.search(r"trillion|\stn", value, re.IGNORECASE))):
				number *= 1000000000000
			elif (bool(re.search(r"billion|\sbn", value, re.IGNORECASE))):
				number *= 1000000000
			elif (bool(re.search(r"million", value, re.IGNORECASE))):
				number *= 1000000
			#looking through the document
			elif(most_freq_unit == 0):
				number *= 1000000000000
			elif(most_freq_unit == 1):
				number *= 1000000000
			elif(most_freq_unit == 2):
				number *= 1000000
			#last resort is guessing based on the size of the number
			elif (1000 > float(re.sub(r"[^\d.]", "", value)) > 1):
				number *= 1000000000
			elif (1000 < float(re.sub(r"[^\d.]", "", value))):
				number *= 1000000
			
			result = float(re.sub(r"[^\d.]", "", re.search(re_digits, value).group(0)))*number
			
			#currency check
			foreign = re.findall(r"EUR|euro|GBP|pounds|CHF|JPY|CNY|CAD|AUD|DKK|BRL|KRW|SEK|[$¢£¤¥֏؋৲৳৻૱௹฿៛\u20AC\u20a0-\u20bd\ua838\ufdfc\ufe69\uff04\uffe0\uffe1\uffe5\uffe6]", doc.page_dict[page], re.IGNORECASE)
			usd = re.findall(r"USD|US dollars?|dollars?|\$", doc.page_dict[page], re.IGNORECASE)
			if (len(foreign) <= len(usd)):
				best.loc[ind] = result
			else:
				best.loc[ind] = max(set(foreign), key=foreign.count) + str(result)


#managing putting items into pandas sereis
def into_series(series, type, value, percent = ''):
	if (bool(re.search(r"equit(?:y|ies)|stocks?", type, re.IGNORECASE))):
		series['equity'+percent] = value
	elif (bool(re.search(r"fixed income|bonds?", type, re.IGNORECASE))):
		series['fixed income'+percent] = value
	elif (bool(re.search(r"multi[- ]asset|hybrid", type, re.IGNORECASE))):
		series['multi-asset'+percent] = value
	elif (bool(re.search(r"alternatives?", type, re.IGNORECASE))):
		series['alternative'+percent] = value
	elif (bool(re.search(r"cash|cash management|money market|liquidity", type, re.IGNORECASE))):
		series['money market'+percent] = value


#pick the best result from a list of results
def evaluate(candidates):
	best = [candidates[0]]
	for i in candidates:
		if (i.count() > best[0].count()):
			best = [i]
		elif(i.count() == best[0].count()):
			best.append(i)
	if (len(best) > 1):
		try:
			#if more with same amount of numbers, let's use the one with the largest numbesr...
			return best[pd.concat(best, axis=1).loc[['equity','fixed income','alternative','multi-asset','money market']].replace('[^\d.]', '', regex=True).astype(float).sum().sort_values(ascending=False).index[0]] 
		except Exception as ex:
			print("########ERRRRRRRORRRRRRRRRR", ex, file=LOG_FILE)
			return best[0]
	else:
		return best[0]


#writing list of results into csv file
def write_csv(series, best):
	output = open('./temp/results'+str(getpid())+'.csv', 'a', encoding='utf-8') if best else open('./temp/candidates'+str(getpid())+'.csv', 'a', encoding='utf-8')
	writer = csv.writer(output, dialect='excel')
	for i in series:
		writer.writerow(i.tolist())
	output.close()


#deleting every file in ./temp with the specified extension
def clean_up(ext):
	for file in [f for f in listdir('./temp') if (isfile(join('./temp', f)) and f.endswith(ext))]:
		remove('./temp/'+file)



if __name__ == "__main__": 
	main()