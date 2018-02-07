
/* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@----QUESTION 1 STARTS -----@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/
/* Business Question 1 : What are the best locations to open new much-needed warehouse cum distribution center? 
   Answer: Top 3 locations best for to open new warehouse cum distribution centers

ZIP 47401	Bloomington	Indiana	    IN	Monroe county	39.1401	-86.5083
ZIP 37922	Knoxville	Tennessee	TN	Knox county 	35.858	-84.1194
ZIP 37205  	Nashville	Tennessee	TN	Davidson county	36.1114	-86.869

*/
LIBNAME Catalog '';

/* Sort the dataset by zipcode */
proc sort data=Catalog.Explo out= Catalog.Sorted_Zipcode;
by  custzip  ;
run;

/* If one order (one record in the dataset) has Qty>1, then the shipping cost in that row is divided by quantity, 
to find the shipping cost for each unit */
Data Catalog.each_Unit_ShipCost;
set Catalog.Explo;
eachunitshipcost = shipnhand/catitmqty;
keep id shipnhand catitmqty custzip eachunitshipcost ;
RUN;
/*sort by zipcode*/
proc sort data=Catalog.each_Unit_ShipCost out= Catalog.Sorted_Zipcode;
by  custzip  ;
run;
/*1) group by zipcode 
  2) Found total number of catalogues ordered in one zipcode
  3) Found average shipping cost to a zipcode (using  shipping cost of each unit in each order which was calculated above)
  4) for each zipcode, multiplied total number of products ordered and avg shipping cost
  5) this way, obtained the grand total shipping cost spent so far for every zipcode
  
*/

Data Catalog.sumQty_avgShipCost (keep = custzip sumQty  avgShipCost);
	Set Catalog.Sorted_Zipcode;
	retain counter sumQty sumShipCost ; 
	By custzip; 
		If first.custzip then  
			do;
				counter = 0;
				sumQty = 0;
				sumShipCost = 0;
				avgShipCost= 0;
			end;
		         
                sumQty = sumQty + catitmqty;


							If eachunitshipcost NE '.' then 
							do;	
                            counter = counter + 1;		
							sumShipCost = sumShipCost + eachunitshipcost;
							end;
				
		If last.custzip  then 
			do;	
				avgShipCost = (sumShipCost/counter);
				If avgShipCost = '.' then 
							do;	
                            avgShipCost = 0;
							end;
				output;
			end;

run;
/* Multiplying of quantity and shippingcost for each zipcode - Tryig to take the highest intersection of both the factors */
Data Catalog.FutureShipCost;
set Catalog.sumQty_avgShipCost;
FutureShipCost = SumQty*avgShipCost;
keep custzip SumQty avgShipCost FutureShipCost ;
RUN;

/*sorted by this totalShippingcost in  descending order*/
proc sort data=Catalog.FutureShipCost out= Catalog.Sorted_FutureShipCost;
by  descending FutureShipCost  ;
run;


proc print data=god.Sorted_futureshipcost;
	
   title 'Total shipping cost spent so far in each zipcode - Expecting to spend alike or more in future';
   footnote '***prices in USD'; 
run;
/* Plotted on US map, the top 30 zipcodes to were shipping was the costliest*/
proc gmap
map=maps.us
data=maps.us (obs=1)
all;
id state;
choro state / nolegend;
run;
quit;

proc GEOCODE plus4 lookup=lookup.zip4
data=Catalog.Sorted_FutureShipCostout=work.geo_address
run;


/* Set the graphics environment */
goptions reset=all cback=white border htitle=12pt htext=10pt;  

 /* Create a data set containing ZIP codes. */
data myzip;
  input Zip;
 cards;
48236
32312
95602
91977
90272
19348
37922
47401
1776
19426
23455
80906
10021
53402
37205
48103
3253
16803
33629
20705
5602
78552
32118
94549
60068
33618
84093
60004
76248
21804
;
run;

 /* Sort the data set by ZIP codes. */
proc sort data=myzip;
  by zip;
run;

 /* Create a data set containing the */
 /* X and Y values for my ZIP codes. */
data longlat;
 /* In case there are duplicate ZIP codes, rename 
    X and Y from the SASHELP.ZIPCODE data set. */
  merge myzip(in=mine) 
        sashelp.zipcode(rename=(x=long y=lat)keep=x y zip);
  by zip;
 /* Keep if the ZIP code was in my data set. */
  if mine;
 /* Convert longitude, latitude in degrees to radians */
 /* to match the values in the map data set. */
  x=atan(1)/45*long;
  y=atan(1)/45*lat;
 /* Adjust the hemisphere */
  x=-x;
  /* Keep only the ZIP, X and Y variables */
  keep zip x y;
run;

 /* Create an annotate data set to place a symbol at the */
 /* ZIP code locations.                                  */
data anno;
 /* Use the X and Y values from the LONGLAT data set. */
  set longlat;
 /* Set the data value coordinate system. */
 /* Set the function to label. */
 /* Set the size of the symbol to .75. */
 /* Set a FLAG variable to signal annotate observations. */
  retain xsys ysys '2' function 'label' size 1.3 flag 1 when 'a';
 /* Set the font to the Special font. */
  style='special';
 /* The symbol is a star. */
  text='M';
 /* Specify the color for the symbol. */
  color='depk';
 /* Output the observation to place the symbol. */
  output;
run;

 /* Combine the map data set with the annotate data set. */
data all;
  /* Subset out the states that you do not want. */
  /* The FIPS code of 2 is Alaska, 15 is Hawaii, */
  /* and 72 is Puerto Rico.  */
  set maps.states(where=(state not in(2 15 72))) anno;
run;

 /* Project the combined data set. */
proc gproject data=all out=allp;
  id state;
run;
quit;

 /* Separate the projected data set into a map and an annotate data set. */
data map dot;
  set allp;
 /* If the FLAG variable has a value of 1, it is an annotate  */
 /* observation; otherwise, it is a map data set observation. */
  if flag=1 then output dot;
  else output map;
run;

 /* Define the pattern for the map. */
pattern1 v=me c=grp r=50;

 /* Define the title for the map. */
title1 'ZIP Code locations on a US Map';

 /* Generate the map and place the symbols at ZIP code locations. */
proc gmap data=map map=map;
  id state;
  choro state / anno=dot nolegend;
run;
quit;
quit; 

/* all are good candidates...
from the map, subjectivey choosing central location and hypothetically testing 
if the avg of shippin in these areas are > avg of shipping in rest 18 areas
*/

data god.top30locations;
set god.sorted_futureshipcost;
if custzip in (
48236,
32312,
95602,
91977,
90272,
19348,
37922,
47401,
1776,
19426,
23455,
80906,
10021,
53402,
37205,
48103,
3253,
16803,
33629,
20705,
5602,
78552,
32118,
94549,
60068,
33618,
84093,
60004,
76248,
21804);
keep custzip sumqty avgshipcost futureshipcost;
run;


/* testing if Mu(shipping cost of all locations) < shipping cost of zipcode# 37922 */
PROC TTEST DATA=god.top30locations H0=229.413 sides = L;
 VAR pastshipcost;
 RUN;
/* testing if Mu(shipping cost of all locations) < shipping cost of zipcode# 47401 */
PROC TTEST DATA=god.top30locations H0=224.412 sides = L;
 VAR pastshipcost;
 RUN;
/* testing if Mu(shipping cost of all locations) < shipping cost of zipcode# 37205 */
/* test failed - so rejecting this region */
PROC TTEST DATA=god.top30locations H0=182.976 sides = L;
 VAR pastshipcost;
 RUN;/* test failed - so rejecting this region */

 /* from 37922 and 47401, one best location can be practically and  subjectively chosen based on the availability 
 of resources and further market research*/

/* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@----QUESTION 1 ENDS -----@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/
 /* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@----QUESTION 2 STARTS -----@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*/
/* Question 2: Which is the best time to introduce discount offers ??

   Parameter considered:  Gross Product Revenue Vs Month

   Test: ANOVA 

  The month where the gross product revenue is less shall be chosen to introduce offers. 
  This way we can trigger sales in the dull month */

/* get zip grossamt n month of all orders*/
Data God.Q3_Dataset1;
set God.Explo;
month=month(ipdt);
keep  CUSTZIP GRPROREVAMT month;
RUN;
/* sort by zip n month*/
PROC SORT DATA=God.Q3_Dataset1 OUT=God.Q3_Dataset2 ;
by CUSTZIP month;
RUN ;
/* group by zip
  sub group by month
  find the total revenue for each zip-month pair*/
Data God.Q3_Dataset3;
set God.Q3_Dataset2;
keep  CUSTZIP GrossRev month;
retain GrossRev;
by CUSTZIP month;
If first.month then /* If first observation for this month, then intislize GrossRev = 0*/
			do;
				GrossRev =0;				
			end;

GrossRev = GrossRev + GRPROREVAMT;
If last.month then /* If last observation for this month, then output */
			do;
								output;
			end;
RUN;
/* testing

H0: Mu(jan) = Mu(feb) = Mu(mar)=.....
H1: Mu(jan) <> Mu(feb) <> Mu(mar) <>.....

So that there is atleast one month when there is less sale..that month shall then be chosen
as the best month for inntroducing offers*/
Proc ANOVA data= God.Q3_dataset3;
	class month;
	model GrossRev = month ;
run;
/* for every zip, jan revenue , feb rev, mar rev, april rev etc are taken*/
Data God.Q3_Dataset4;
set God.Q3_Dataset3;
keep  CUSTZIP Jan feb mar apr may jun jul aug sep oct nov dec;
retain Jan feb mar apr may jun jul aug sep oct nov dec;
by CUSTZIP ;     If first.CUSTZIP then do;
				    Jan=0;  feb=0; mar=0; apr=0; may=0;jun=0; jul=0; aug=0; sep=0; oct=0; nov=0; dec=0;
				 end;
				 If month = 1 then do;
                    If GrossRev > 0 then do; Jan = GrossRev;  end;
			     end;
				 If month = 2 then do;
                    If GrossRev > 0 then do; feb = GrossRev;  end;
			     end;
				 If month = 3 then do;
                    If GrossRev > 0 then do; mar = GrossRev;  end;
			     end;
				 If month = 4 then do;
                    If GrossRev > 0 then do; apr = GrossRev;  end;
			     end;
				 If month = 5 then do;
                    If GrossRev > 0 then do; may = GrossRev;  end;
			     end;
				 If month = 6 then do;
                    If GrossRev > 0 then do; jun = GrossRev;  end;
			     end;
				 If month = 7 then do;
                    If GrossRev > 0 then do; jul = GrossRev;  end;
			     end;
				 If month = 8 then do;
                    If GrossRev > 0 then do; aug = GrossRev;  end;
			     end;
				 If month = 9 then do;
                    If GrossRev > 0 then do; sep = GrossRev;  end;
			     end;
				 If month = 10 then do;
                    If GrossRev > 0 then do; oct = GrossRev;  end;
			     end;
				 If month = 11 then do;
                    If GrossRev > 0 then do; nov = GrossRev;  end;
			     end;
				 If month = 12 then do;
                    If GrossRev > 0 then do; dec = GrossRev;  end;
			     end;
				 if last.CUSTZIP then do;
				 if jan = 0 then jan = .;
				 if feb = 0 then feb = .;
				 if mar = 0 then mar = .;
				 if apr = 0 then apr = .;
				 if may = 0 then may = .;
				 if jun = 0 then jun = .;
				 if jul = 0 then jul = .;
				 if aug = 0 then aug = .;
				 if sep = 0 then sep = .;
				 if oct = 0 then oct = .;
				 if nov = 0 then nov = .;
				 if dec = 0 then dec = .;
				    output;
				 end;
RUN;
/* Mean revenue for each month */

proc means data= God.Q3_dataset4 n mean max min range std;
 
   var jan feb mar apr may jun jul aug sep oct nov dec;

   title 'Summary of Monthly Revenues of Several zipcodes';
run;
/* ttest to chk if jan mean is less than all other month mean */
Data god.Q3_dataset5;
set god.Q3_dataset3;
IF grossrev NE 0;
keep custzip grossrev month Is_Jan;
If month NE 1 then Is_Jan = 'not jan';
else if month = 1 then IS_Jan = 'Jan';
run;
/* mean of jan is less than mean of non jan- see the mean in t test result*/
proc ttest data=god.Q3_dataset5 sides = L;
 class Is_Jan;
 var grossrev;
 run;

 /* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@----Question 2: ENDS-----@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ */
  /* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ ----Question 3: STARTS-----@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ */
 /* Business Question 3: CAn the organisation do some cost cutting on priting catalog by adopting web catalog??
    Answer: yes*/

 data god.Q1_printVSWeb_1;
 set god.explo;
 keep ordnum GRPROREVAMT catitmind catitmqty webitid webitqty printORweb qty year;
 year = year(ipdt);
 if catitmind = 'Y' and webitid = 'N' then printORWeb = 'printedCatalogOrder';
 if catitmind = 'N' and webitid = 'Y' then printORWeb = 'webCatalogOrder';
 if catitmind = 'Y' and webitid = 'Y' then printORWeb = 'both';
 if catitmind = 'N' and webitid = 'N' then printORWeb = 'neither';
 qty = catitmqty + webitqty;
 run;


  proc sort data=god.Q1_printVSWeb_1;
  by printORweb;
  run;

 data god.Q1_printVSWeb_2;
 set god.Q1_printVSWeb_1;
 keep ordnum GRPROREVAMT catitmind catitmqty webitid webitqty printORweb qty year;
 if printORweb NE 'neither' and printORweb NE 'both' and qty NE 0;
 run;
  
PROC SORT DATA=God.Q1_printVSWeb_2 OUT=God.Q1_printVSWeb_3 ;
BY qty;
RUN ;
/* perfect  correlation- 
   year increases -> catitmqty decreases
   year increases -> webitqty increases 
tHe organisation can confidently switch their investment plan from prinnt ctatalog to web catalog- 
that will be lot of cost cutting for them*/
ods graphics on;
title 'Chi- Squared test of independence between year and printORweb';
proc corr data=God.Q1_printVSWeb_3 sscp cov plots=matrix;
   var  catitmqty webitqty  ;
   with year ;
run;
ods graphics off;

