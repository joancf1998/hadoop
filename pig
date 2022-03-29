REGISTER /usr/lib/pig/piggybank.jar;
extract_details = LOAD '/user/cloudera/pig_analisis_opinions/critiquescinematografiques.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (text:chararray, label:int, id:int);
tokens = foreach extract_details generate id,label,text, FLATTEN(TOKENIZE(text)) As word;
dictionary = load '/user/cloudera/pig_analisis_opinions/AFINN.txt' using PigStorage('\t') AS(word:chararray,rating:int);
word_rating = join tokens by word left outer, dictionary by word using 'replicated';
describe word_rating;
rating = foreach word_rating generate tokens::id as id,tokens::text as text, tokens::label as label, dictionary::rating as rate;
word_group = group rating by (id,text,label);

avg_rate = foreach word_group generate group, AVG(rating.rate) as AVG;
comp3 = foreach avg_rate generate group, ((AVG>0)? 1 : 0) as avg_positiu:int, AVG;
/* dump comp3; */
comp4 = foreach avg_rate generate group, ((group.label==0)? 1 : 0) as no_label:int, AVG;
/* dump comp4; */
STORE avg_rate_group_max_min INTO '/user/cloudera/pig_analisis_opinions/resultat_analisis_opinions_max_min' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');

