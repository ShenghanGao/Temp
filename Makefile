ua:
	sbatch submit-ua.sh $(input)

au:
	sbatch submit-au.sh $(input)

co:
	sbatch submit-co.sh $(input)

co-p:
	sbatch submit-co-pearson.sh $(input)

re:
	sbatch submit-re.sh $(matrix) $(user)

toseq:
	sbatch submit-toseq.sh $(input)

cleanseq:
	rm -rf data/convertedOut

cleanout:
	rm -f out/*.txt

cleanlog:
	rm -f *.out

cleanall: cleanseq cleanout cleanlog
