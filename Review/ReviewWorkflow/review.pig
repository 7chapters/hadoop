review = load '$INPUT' using PigStorage(',') 
	as (category:chararray ,hash:chararray, url:chararray , positive:int , negative:int ,totalreview:int );

url_cat = foreach review generate hash ,TRIM(category); 
store url_cat into '$pigCategoryOutputDir';

url_review =  foreach review generate  hash  , url , positive, negative, totalreview; 
url_review_distict = distinct url_review;
store url_review_distict into '$pigReviewDistinctOutputDir';
