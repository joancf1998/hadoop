REGISTER /usr/lib/pig/piggybank.jar;

extract_details = LOAD '/user/cloudera/pig_analisis_opinions/critiquescinematografiques.csv' 
 USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  
 AS (text:chararray, label:int, id:int);

tokens = foreach extract_details generate id,label,text, FLATTEN(TOKENIZE(text)) As word;
dictionary = load '/user/cloudera/pig_analisis_opinions/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);
word_rating = join tokens by word left outer, dictionary by word using 'replicated';

; describe word_rating;
rating = foreach word_rating generate tokens::id as id,tokens::text as text, tokens::label as label, dictionary::rating as rate;
word_group = group rating by (id,text,label);
avg_rate = foreach word_group generate group, AVG(rating.rate) as AVG;

comp = foreach avg_rate generate group, (((AVG>=0) AND (group.label==1)) OR ((AVG<0) AND (group.label==0))? true : false) as c:boolean, AVG;

compt5 = foreach comp generate group.text as text, group.label as label, AVG, c;

all_c = GROUP compt5 by c,
count_all_neg = FOREACH all_c
GENERATE FLATTEN(group) as (all_c), COUNT($1);

STORE compt5 INTO '/user/cloudera/pig_analisis_opinions/bb' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')
