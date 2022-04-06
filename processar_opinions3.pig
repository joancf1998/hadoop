REGISTER /usr/lib/pig/piggybank.jar;

extract_details = LOAD '/user/cloudera/pig_analisis_opinions/critiquescinematografiques.csv' 
 USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  
 AS (text:chararray, label:int, id:int);

tokens = foreach extract_details generate id,label,text, FLATTEN(TOKENIZE(text)) As word;
dictionary = load '/user/cloudera/pig_analisis_opinions/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);
word_rating = join tokens by word left outer, dictionary by word using 'replicated';

rating = foreach word_rating generate tokens::id as id,tokens::text as text, tokens::label as label, dictionary::rating as rate, ((dictionary::rating>=0)? 1 : 0) as paraula_positiva:int, ((dictionary::rating<0)? 1 : 0) as paraula_negativa:int;

word_group = group rating by (id,text,label);

avg_rate = foreach word_group generate group, AVG(rating.rate) as AVG, SUM(rating.paraula_positiva) as paraules_positives, SUM(rating.paraula_negativa) as paraules_negatives;

comp = foreach avg_rate generate group, (((AVG>=0) AND (group.label==1)) OR ((AVG<0) AND (group.label==0))? true : false) as c:boolean, AVG as opinio_optinguda, paraules_negatives, paraules_positives;

comp5 = foreach comp generate group.text as text, group.label as label, opinio_optinguda as opinio_optinguda, paraules_positives as nparaules_positives, paraules_negatives as nparaules_negatives, group.label as label2, c as comparacio;

STORE comp5 INTO '/user/cloudera/pig_analisis_opinions/cc' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
