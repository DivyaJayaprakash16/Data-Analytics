/* Import mailed.csv into sastable from fourth merge */ 

libname iter2 ''; 



PROC IMPORT OUT= iter2.MAILED 
            DATAFILE= "\\smpnas02\MKT6337\TheSuperlatives\DominoPizza\NewData\FourthMerge\mailed.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data iter2.Mailed;
set iter2.Mailed;
if  AddressId = . then delete;
run;
/*data  iter2.transactions4;
set iter2.transactions4;
drop outtime;
run;*/

/*appending all transaction files into one Log_Trans_V1*/
data iter2.Log_Trans_V1(keep=addressid zip5 storenum dateofOrder ordertype OrderAmount menuamount domcustid allbreadcount wkday coupon mealperiod AllBreadCount ChickenOrd DrinkOrd DessertCount);
set iter2.transactions1 iter2.transactions2 iter2.transactions3 iter2.transactions4;
if couponcode = ' ' then coupon = 0 ; else coupon = 1;
t=1;
by addressid;
run;
data iter2.Log_Trans_V1;
set iter2.Log_Trans_V1;
if addressid = . then delete;
run;


/* sorting mailed and Log_Trans_V1 file on addressedid */
proc sort data = iter2.Mailed;
by AddressId;
run;
proc sort data = iter2.Log_Trans_V1;
by AddressId;
run;


/*merging mailed and transactions */
data iter2.log_M_T_v1;
merge iter2.Mailed iter2.Log_Trans_V1 ;
by addressid;
/*keep addressid StoreNum MarketingSpend inhomedate CustType zip5 OrderAmount DateOfOrder mailed Mail_date;*/
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
run;

/* marking missing date of order -1 varname- NDOO ; and customers in mailes with no transactions as 1 varname - NTRANS*/
data iter2.log_M_T_v2;
set iter2.log_M_T_v1;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;



/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 3533941 */
data iter2.x;
set iter2.log_M_T_v2;
where NDOO = 1 and NTRANS = 0;
run;


data iter2.x;
set iter2.x ;
x = 1; /*adding extra column as flag to those address ids where NDOO=1 and NTRANS =0*/
run;
proc sort data = iter2.x;
by addressid;
run;
proc sort data = iter2.log_M_T_v2; /*count =  8664496*/
by addressid;
run;
data iter2.y(keep = addressid x);*merging x data set and log_M_T_v2, count =  8664496 , droping all other variables and keeping only addressid and x flag,
xflag represents adressids coming from x table;
merge iter2.log_M_T_v2 iter2.x;
by addressid;
run;
proc means data =  iter2.y N Nmiss; *count N(x=1) = 5661234 , NMISS(x=.)= 3003262 ;
var x;
run; 
/*deleteing all the address id with any observation satisfying NDOO=1 and NTRANS =0 condition*/
data iter2.z;
set iter2.y;
if x=1 then delete;
z =1; *z flag represents those address id where x is missing i.e. do not comply to NDOO=1 and NTRANS =0 condition, count= 3003262; 
run;
/*merging z dataset and me_merge1 and deleting those records corresponding to address ids which do not come from z dataset (z =.)*/
data iter2.log_M_T_v3;
merge iter2.z iter2.log_M_T_v2;
by addressid;
if z = '.' then delete; *count = 3003262;
run;
data iter2.log_M_T_v3 (drop= x z ndoo ntrans inhomedate);
set iter2.log_M_T_v3;
run;
/*checking if pullid = 0 in any observations. And there is no observation with pull id= 0*/
proc sort data = iter2.mt_F_merge ;
by pullid;
run;
/*proc means data = iter2.mt_F_merge N Nmiss;
var pullid;
run;*/
/*retriving date part as mail_date from in home date
data iter2.log_M_T_v3;
set  iter2.log_M_T_v3;
Mail_date=datepart(inhomedate);
format Mail_date date9.;
if Mail_date = . then delete;
drop inhomedate;
run;
*/

/*keeping only historic transactions */
data iter2.log_M_T_v4;
set iter2.log_M_T_v3;
where dateoforder < mail_date;
run;

proc sort data = iter2.demographics_big;
by addressid;
run;

data iter2.Log_demog_v1;
set iter2.demographics_big;
keep addressid occupancycount creditcard_holder maritalstatus numberofadults numberofchildren
ppi heavy_internet_user lhi_general_sports lhi_cooking;
laddressid = lag(addressid);
if laddressid = addressid then delete;
run;




data iter2.M_T_merge_2;
set iter2.M_T_merge;
if pullid= . then delete;
if dateoforder = . then delete;
run;

data iter2.M_T_merge_2;
set  iter2.M_T_merge_2;
Mail_date=datepart(inhomedate);
format Mail_date date9.;
if Mail_date = . then delete;
drop inhomedate;
run;

/*Removing recordxs with emptgy order date in the transaction table*/
data iter2.M_T_merge_3;
set iter2.M_T_merge_2;
if dateoforder< mail_date then Order_Before = 1; else Order_before = 0;
if dateoforder> (mail_date + 30) then Order_after = 1; else Order_after = 0;
if dateoforder> mail_date and dateoforder< (mail_date + 31)  then Order_within_30 = 1; else Order_within_30 = 0;
run;

/*creating histogram for date mailed */ 
proc univariate data = iter2.M_T_merge_3;
histogram;
var Mail_Date ;
run;

/*Importing control.csv*/

PROC IMPORT OUT= iter2.control_master
            DATAFILE= "\\smpnas02\MKT6337\TheSuperlatives\DominoPizza\Iteration2\control.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
/*merging control files control 1 -- control4
data iter2.control_master;
set iter2.control1 iter2.control1 iter2.control3 iter2.control4;
run;*/


/*merging master control with transactions*/
proc sort data = iter2.control_master;
by addressid;
run;
data iter2.C_T_merge;
merge iter2.control_master iter2.transactions ;
by addressid;
keep addressid DomCustId StoreNum MarketingSpend inhomedate CustType zip5 OrderAmount DateOfOrder mailed Mail_date wkday;
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
run;

data iter2.C_T_merge_1;
set  iter2.C_T_merge;
if custtype = ' ' then delete;
run;



/* Control  */

*checking for null custtype and pullid;
data iter2.control_master;
set iter2.control_master;
if custtype ='' then c =1 ;
run;

proc means data = iter2.control_master N Nmiss;
var c;
run;
data iter2.control_master;
set iter2.control_master;
if pullid = . then delete;
if custtype = ' ' then delete;
run;

/*merging master control with master_trans*/
proc sort data = iter2.control_master;
by addressid;
run;
/*DATA iter2.master_trans (drop = custtype);
set iter2.master_trans;
run;*/
data iter2.log_C_T_v1;
merge iter2.control_master iter2.Log_Trans_V1;
by addressid;
/*keep pullid addressid DomCustId StoreNum MarketingSpend  CustType zip5 menuamount OrderAmount DateOfOrder mailed Mail_date wkday ;*/
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
drop inhomedate;
run;

data iter2.log_C_T_v1;
set iter2.log_C_T_v1;
if custtype ='' then c =1 ;
run;
data iter2.log_C_T_v2;
set iter2.log_C_T_v1;
if c = 1 then delete;
run;
/* marking missing date of order -1 varname- NDOO ; and customers in mailes with no transactions as 1 varname - NTRANS*/
data iter2.log_C_T_v3;
set iter2.log_C_T_v2;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;
/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 208988*/
data iter2.x_c;
set iter2.log_C_T_v3;
where NDOO = 1 and NTRANS = 0;
run;
data iter2.x_c;
set iter2.x_c ;
x = 1; /*adding extra column as flag to those address ids where NDOO=1 and NTRANS =0*/
run;
proc sort data = iter2.x_c;
by addressid;
run;
proc sort data = iter2.log_C_T_v3; /*count =  506135*/
by addressid;
run;
data iter2.y_c(keep = addressid x);*merging x_c data set and c_t_merge_2, count =  506135 , droping all other variables and keeping only addressid and x flag,
xflag represents adressids coming from x_c table;
merge iter2.log_C_T_v3 iter2.x_c;
by addressid;
run;
proc means data =  iter2.y_c N Nmiss; *count N(x=1) = 346803   , NMISS(x=.)= 159332 ;
var x;
run;
/*deleteing all the address id with any observation satisfying NDOO=1 and NTRANS =0 condition*/
data iter2.z_c;
set iter2.y_c;
if x=1 then delete;
z =1; *z flag represents those address id where x is missing i.e. do not comply to NDOO=1 and NTRANS =0 condition, count= 159332; 
run;
/*merging z dataset and c_t_merge_2 and deleting those records corresponding to address ids which do not come from z dataset (z =.)*/
data iter2.log_C_T_v4;
merge iter2.z_c iter2.log_C_T_v3;
by addressid;
if z = '.' then delete; *count = 159332;
run;
data iter2.log_C_T_v5 (drop= c pullid x z ndoo ntrans);
set iter2.log_C_T_v4;
run;
/* merging demographics and mailed*/
proc sort data = iter2.log_M_T_v5;
by addressid;
run;

proc sort data = iter2.log_demog_v1;
by addressid;
run;

data iter2.log_MTD_v1;
merge iter2.log_M_T_v5 iter2.log_demog_v1;
by addressid;
run;

data iter2.log_mtd_v2;
set iter2.log_mtd_v1;
if custtype= ' ' then delete;
run;

/*merging demo and control*/
proc sort data = iter2.log_c_t_v5;
by addressid;
run;

proc sort data = iter2.log_demog_v1;
by addressid;
run;

data iter2.log_CTD_v1;
merge iter2.log_C_T_v5 iter2.log_demog_v1;
by addressid;
run;
data iter2.log_ctd_v2;
set iter2.log_ctd_v1;
if custtype= ' ' then delete;
run;
******************************************************************************************************************************

libname dis '';

data iter2.Mailed;
set iter2.Mailed;
if  AddressId = . then delete;
run;
/*data  iter2.transactions4;
set iter2.transactions4;
drop outtime;
run;*/

/*appending all transaction files into one Log_Trans_V1*/
data iter2.Log_Trans_V1(keep=addressid zip5 storenum dateofOrder ordertype OrderAmount menuamount domcustid allbreadcount wkday coupon mealperiod AllBreadCount ChickenOrd DrinkOrd DessertCount);
set iter2.transactions1 iter2.transactions2 iter2.transactions3 iter2.transactions4;
if couponcode = ' ' then coupon = 0 ; else coupon = 1;
t=1;
by addressid;
run;
data iter2.Log_Trans_V1;
set iter2.Log_Trans_V1;
if addressid = . then delete;
run;


/* sorting mailed and Log_Trans_V1 file on addressedid */
proc sort data = iter2.Mailed;
by AddressId;
run;
proc sort data = iter2.Log_Trans_V1;
by AddressId;
run;


/*merging mailed and transactions */
data iter2.log_M_T_v1;
merge iter2.Mailed iter2.Log_Trans_V1 ;
by addressid;
/*keep addressid StoreNum MarketingSpend inhomedate CustType zip5 OrderAmount DateOfOrder mailed Mail_date;*/
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
run;

/* marking missing date of order -1 varname- NDOO ; and customers in mailes with no transactions as 1 varname - NTRANS*/
data iter2.log_M_T_v2;
set iter2.log_M_T_v1;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;



/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 3533941 */
data dis.mtpizza1;
set iter2.log_M_T_v2;
if NDOO = 1 and NTRANS = 0 then delete;
run;

/*Historic transactions*/
data dis.mtpizza2;
set dis.mtpizza1;
drop NDOO  NTRANS;
where dateoforder < mail_date;
run;

data dis.mtpizza3;
set dis.mtpizza2;
if custtype = 'New' or custtype = 'New OL' or custtype = 'New NOL' then Recency_Score = 6;

else if custtype = 'MVP' or custtype = 'MVP OL' or custtype = 'MVP NOL' 
or custtype = 'Frequent' or custtype = 'Frequent OL' or custtype = 'Frequent NOL' 
or custtype = 'Rejuvenated' or custtype = 'Rejuvenated OL' or custtype = 'Rejuvenated NOL' then Recency_Score = 5;

else if custtype = 'At Risk' or custtype = 'At Risk OL' or custtype = 'At Risk NOL' then Recency_Score = 4;
else if custtype = 'High Risk' or custtype = 'High Risk OL' or custtype = 'High Risk NOL' then Recency_Score = 3;
else if custtype = 'Max Risk' or custtype = 'Max Risk OL' or custtype = 'Max Risk NOL' then Recency_Score = 2;
else if custtype = 'Lost' or custtype = 'Lost OL' or custtype = 'Lost NOL' then Recency_Score = 1;
else Recency_Score = 3.5;
run;


data dis.mtpizza4;
set dis.mtpizza3;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_transaction = 0;
else Bfr_3month_transaction = 1;
drop inhomedate creative piecetype;
run;



/*lift calculation*/




data dis.mtlift1;
set dis.mtpizza1;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then after_mail = 1; else after_mail = 0;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then before_mail = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;

/*data  iter2.M_t_merge_4; 
set iter2.M_t_merge_4;
if mail_date=. then delete; 
run; */
 

proc sort data =  dis.mtlift1; 
by AddressId PullId;  
run;
 
/* To group AddressId and mail date and lots of other things*/   
data dis.mtlift2;
set dis.mtlift1; 
/*keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;*/
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;

data dis.mtlift3;
set  dis.mtlift2;
lift = avg_menu_amt_aft - avg_menu_amt_bfr;
if lift > 0 then response =1; else response =0;
keep addressid pullid mail_date avg_menu_amt_Bfr avg_menu_amt_aft lift response;
run;

proc sort data = dis.mtpizza4;  
by addressid pullid;
run;

proc sort data = dis.mtlift3;
by addressid pullid;
run;

data dis.mtpizza5;
merge dis.mtpizza4  dis.mtlift3;
by addressid pullid;
run;
proc sort data = dis.mtpizza5;
by addressid pullid;
quit;
/* In  */

/*data dis.mtlift1;
set dis.mtpizza1;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then purchase = 1;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then purchase = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;
 */
data dis.mtpizza6;
set dis.mtpizza5;
drop ordertype wkday mealperiod coupon;
run;

data dis.mtpizza7;
set dis.mtpizza6;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_menuAmt = 0;
else Bfr_3month_menuAmt = MenuAmount;
run; 

data dis.mtpizza8;
set dis.mtpizza7;
if marketingspend = . then marketingspend =0;
if menuamount = . then menuamount =0;
if Bfr_3month_transaction = . then Bfr_3month_transaction =0;
if orderamount = . then orderamount =0;
if menuamount = . then menuamount =0;
if allbreadcount = . then allbreadcount =0;
if chickenord = . then chickenord =0;
if dessertcount = . then dessertcount =0;
if drinkord = . then drinkord =0;
if recency_score =. then recency_score = 0;
run;
/*aggregating transation variables*/

data dis.mtpizza9;
set dis.mtpizza8;
retain count s_marketingspend s_menuamount s_Bfr_3month_transaction s_Bfr_3month_menuAmt s_orderamount s_menuamount s_allbreadcount s_chickenord s_dessertcount
s_drinkord s_recency_score;
by addressid pullid;
if first.pullid then 
do
count = 0;
s_marketingspend =0;
s_menuamount =0;
s_Bfr_3month_transaction =0;
s_Bfr_3month_menuAmt =0;
s_orderamount =0;
s_menuamount =0;
s_allbreadcount =0;
s_chickenord =0;
s_dessertcount=0;
s_drinkord =0;
s_recency_score=0;
end;
count = count + 1;
keep addressid pullid storenum custtype zip5 mail_date Avgmarketingspend Avgmenuamount AvgBfr_3month_transaction AvgBfr_3month_menuAmt Avgorderamount Avgmenuamount Avgallbreadcount Avgchickenord Avgdessertcount
Avgdrinkord Avgrecency_score lift response;

s_marketingspend = s_marketingspend + marketingspend;
s_menuamount = s_menuamount + menuamount;
s_Bfr_3month_transaction = s_Bfr_3month_transaction + Bfr_3month_transaction;
s_Bfr_3month_menuAmt = s_Bfr_3month_menuAmt + Bfr_3month_menuAmt;
s_orderamount = s_orderamount + orderamount;
s_allbreadcount = s_allbreadcount + allbreadcount;
s_drinkord = s_drinkord + drinkord;
s_recency_score = s_recency_score + recency_score;

if last.pullid then
do
Avgmarketingspend = s_marketingspend / count;
AvgBfr_3month_transaction = s_Bfr_3month_transaction/count;
AvgBfr_3month_menuAmt = s_Bfr_3month_menuAmt/ count;
Avgorderamount = s_orderamount / count;
Avgmenuamount = s_menuamount / count;
Avgallbreadcount = s_allbreadcount / count;
Avgchickenord = s_chickenord / count;
Avgdessertcount = s_dessertcount / count;
Avgdrinkord = s_drinkord / count;
Avgrecency_score = s_recency_score / count;
output;
end; 
run;

data dis.mtdpizza_v1;
merge dis.mtpizza9 (in = mt) dis.log_demog_v1 (in = demog);
by addressid;
mt1 = mt;
demog1 = demog;
run;

data dis.mtdpizza_v2;
set dis.mtdpizza_v1;
if mt1= 0 and demog1 =1 then delete;
run;

libname dis '';

proc sql;
select * from dis.mtdpizza_v2 where pullid = 43001 and addressid = 10474191;
quit;


proc corr data=dis.mtdpizza_v2;

VAR Avgmarketingspend 
Avgmenuamount
AvgBfr_3month_transaction 
AvgBfr_3month_menuAmt 
Avgorderamount

Avgallbreadcount 
Avgchickenord 
Avgdessertcount
Avgdrinkord 
Avgrecency_score
occupancycount

numberofadults
numberofchildren
heavy_internet_user
lhi_cooking
lhi_general_sports
ppi
BY response;
run;

PROC MEANS data=dis.mtdpizza_v2 N NMISS MEAN STDDEV MIN P25 MEDIAN P75 MAX;
	VAR Avgmarketingspend 
Avgmenuamount
AvgBfr_3month_transaction 
AvgBfr_3month_menuAmt 
Avgorderamount
Avgmenuamount
Avgallbreadcount 
Avgchickenord 
Avgdessertcount
Avgdrinkord 
Avgrecency_score
occupancycount
creditcardholder
maritalstatus
numberofadults
numberofchildren
heavy_internet_user
lhi_cooking
lhi_general_sports
ppi
response;
	TITLE 'Summary Statistics';
	
RUN;
data dis.mtdpizza_v3;
set dis.mtdpizza_v2;
int1 = ppi*numberofchildren;
int2 = ppi*lhi_cooking;
run;
/*
PROC REG DATA = dis.mtdpizza_v2;
     
	MODEL response = ppi numberofchildren lhi_cooking int1 int2;
	
	BY addressid pullid;
	
RUN;
*/
ods graphics on;
proc logistic data= dis.mtdpizza_v3 plots;*(MAXPOINTS=NONE)=(roc(id=obs) effect); 
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb ;
run;
ods graphics off;


/*Control*/
 
proc sort data = dis.control_master;
by addressid;
run;


proc sort data = dis.Log_Trans_V1;
by addressid;
run;

data dis.ctpizza_v1; 
merge dis.control_master(in = control_master) dis.Log_Trans_V1(in =Log_Trans_V1 );
by addressid ;
control_masterInd =control_master;
Log_Trans_V1Ind =Log_Trans_V1;
run;

data dis.ctpizza_v2;
set dis.ctpizza_v1; 
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
if control_masterInd = 0 and Log_Trans_V1Ind = 1 then delete;
run;

data dis.ctpizza_v3;
set dis.ctpizza_v2;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;

/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 3533941 */
data dis.ctpizza_v4;
set dis.ctpizza_v3;
if NDOO = 1 and NTRANS = 0 then delete;
run;


/*Historic transactions*/
data dis.ctpizza_v5;
set dis.ctpizza_v4;
drop NDOO  NTRANS;
where dateoforder < mail_date;
run;

data dis.ctpizza_v6;
set dis.ctpizza_v5;
if custtype = 'New' or custtype = 'New OL' or custtype = 'New NOL' then Recency_Score = 6;

else if custtype = 'MVP' or custtype = 'MVP OL' or custtype = 'MVP NOL' 
or custtype = 'Frequent' or custtype = 'Frequent OL' or custtype = 'Frequent NOL' 
or custtype = 'Rejuvenated' or custtype = 'Rejuvenated OL' or custtype = 'Rejuvenated NOL' then Recency_Score = 5;

else if custtype = 'At Risk' or custtype = 'At Risk OL' or custtype = 'At Risk NOL' then Recency_Score = 4;
else if custtype = 'High Risk' or custtype = 'High Risk OL' or custtype = 'High Risk NOL' then Recency_Score = 3;
else if custtype = 'Max Risk' or custtype = 'Max Risk OL' or custtype = 'Max Risk NOL' then Recency_Score = 2;
else if custtype = 'Lost' or custtype = 'Lost OL' or custtype = 'Lost NOL' then Recency_Score = 1;
else Recency_Score = 3.5;
run;


data dis.ctpizza_v7;
set dis.ctpizza_v6;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_transaction = 0;
else Bfr_3month_transaction = 1;
drop inhomedate creative piecetype;
run;

proc sql;
select count(*) from  
dis.ctlift1 where after_mail =1 ;
quit;
/*lift calculation*/

data dis.ctlift1;
set dis.ctpizza_v4;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then after_mail = 1; else after_mail = 0;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then before_mail = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;

/*data  iter2.M_t_merge_4; 
set iter2.M_t_merge_4;
if mail_date=. then delete; 
run; */
 

proc sort data =  dis.ctlift1; 
by AddressId PullId;  
run;
 
/* To group AddressId and mail date and lots of other things*/   
data dis.ctlift2;
set dis.ctlift1; 
/*keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;*/
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;

data dis.ctlift3;
set  dis.ctlift2;
lift = avg_menu_amt_aft - avg_menu_amt_bfr;
if lift > 0 then response =1; else response =0;
keep addressid mail_date pullid avg_menu_amt_Bfr avg_menu_amt_aft lift response;
run;
/*endol*/
proc sort data = dis.ctpizza_v7;  
by addressid pullid;
run;

proc sort data = dis.ctlift3;
by addressid pullid;
run;

data dis.ctpizza_v8;
merge dis.ctpizza_v7  dis.ctlift3;
by addressid pullid;
run;

 
data dis.ctpizza_v9;
set dis.ctpizza_v8;
drop ordertype wkday mealperiod coupon;
run;

data dis.ctpizza_v10;
set dis.ctpizza_v9;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_menuAmt = 0;
else Bfr_3month_menuAmt = MenuAmount;
run; 

data dis.ctpizza_v11;
set dis.ctpizza_v10;
if marketingspend = . then marketingspend =0;
if menuamount = . then menuamount =0;
if Bfr_3month_transaction = . then Bfr_3month_transaction =0;
if orderamount = . then orderamount =0;
if menuamount = . then menuamount =0;
if allbreadcount = . then allbreadcount =0;
if chickenord = . then chickenord =0;
if dessertcount = . then dessertcount =0;
if drinkord = . then drinkord =0;
if recency_score =. then recency_score = 0;
run;
/*aggregating transation variables*/

data dis.ctpizza_v12;
set dis.ctpizza_v11;
retain count s_marketingspend s_menuamount s_Bfr_3month_transaction s_Bfr_3month_menuAmt s_orderamount s_menuamount s_allbreadcount s_chickenord s_dessertcount
s_drinkord s_recency_score;
by addressid pullid;
if first.pullid then 
do
count = 0;
s_marketingspend =0;
s_menuamount =0;
s_Bfr_3month_transaction =0;
s_Bfr_3month_menuAmt =0;
s_orderamount =0;
s_menuamount =0;
s_allbreadcount =0;
s_chickenord =0;
s_dessertcount=0;
s_drinkord =0;
s_recency_score=0;
end;
count = count + 1;
keep addressid pullid storenum custtype zip5 mail_date Avgmarketingspend Avgmenuamount AvgBfr_3month_transaction AvgBfr_3month_menuAmt Avgorderamount Avgmenuamount Avgallbreadcount Avgchickenord Avgdessertcount
Avgdrinkord Avgrecency_score lift response;

s_marketingspend = s_marketingspend + marketingspend;
s_menuamount = s_menuamount + menuamount;
s_Bfr_3month_transaction = s_Bfr_3month_transaction + Bfr_3month_transaction;
s_Bfr_3month_menuAmt = s_Bfr_3month_menuAmt + Bfr_3month_menuAmt;
s_orderamount = s_orderamount + orderamount;
s_allbreadcount = s_allbreadcount + allbreadcount;
s_drinkord = s_drinkord + drinkord;
s_recency_score = s_recency_score + recency_score;

if last.pullid then
do
Avgmarketingspend = s_marketingspend / count;
AvgBfr_3month_transaction = s_Bfr_3month_transaction/count;
AvgBfr_3month_menuAmt = s_Bfr_3month_menuAmt/ count;
Avgorderamount = s_orderamount / count;
Avgmenuamount = s_menuamount / count;
Avgallbreadcount = s_allbreadcount / count;
Avgchickenord = s_chickenord / count;
Avgdessertcount = s_dessertcount / count;
Avgdrinkord = s_drinkord / count;
Avgrecency_score = s_recency_score / count;
output;
end; 
run;

data dis.ctdpizza_v1;
merge dis.ctpizza_v12 (in = ct) dis.log_demog_v1 (in = demog);
by addressid;
ct1 = ct;
demog1 = demog;
run;

data dis.ctdpizza_v2;
set dis.ctdpizza_v1;
if ct1= 0 and demog1 =1 then delete;
run;

data dis.ctdpizza_v3;
set dis.ctdpizza_v2;
int1 = ppi*numberofchildren;
*int2 = ppi*lhi_cooking;
run;
libname dis '';
ods graphics on;
proc logistic data= dis.ctdpizza_v4; /* plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDregOutput predicted = pred;
run;
ods graphics off;
 
proc sort data = dis.CTDregOutput;
by descending pred;
run;;

proc sort data = dis.ctdpizza_v4;
by response;
run;


proc surveyselect data=dis.ctdpizza_v4
         method=srs n= 1809
         seed=10000 out=dis.ctdpizza_stratifed_v1;
      strata response;
   run;



ods graphics on;
proc logistic data= dis.ctdpizza_stratifed_v1  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); 
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDStratRegOutput predicted = pred;
run;
ods graphics off;

/*MTD REGRESSION*/

ods graphics on;
proc logistic data= dis.mtdpizza_v3; /*  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  	response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb ;
OUTPUT OUT= dis.MTDRegOutput predicted = pred;
run;
ods graphics off;

data dis.mtdpizza_v4;
set dis.mtdpizza_v3;
if custtype = ' ' then delete;
run;

proc sort data = dis.MTDregOutput;
by descending pred;
quit;

proc sort data = dis.mtdpizza_v4;
by response;
quit;



proc surveyselect data=dis.mtdpizza_v4
         method=srs n= 75406
         seed=10000 out=dis.mtdpizza_stratifed_v1;
      strata response;
   run;


/* baset creation */
   data dis.mtdpizza_v4;
set dis.mtdpizza_v3;
if numberofadults = . then numberofadults = 0;
if numberofchildren = . then numberofchildren = 0;
if ppi = . then ppi = 0;
householdmembercount = numberofadults + numberofchildren;
run;
proc sort data = dis.mtdpizza_v4;
by mail_date;
run;
proc sql;
create table dis.mtdpizza_v5 as 
select mail_date, median(ppi) as median_ppi,median(householdmembercount) as medianMemCount from dis.mtdpizza_v4 group by mail_date;
quit;

/* basket creation for control*/
 data dis.ctdpizza_v4;
set dis.ctdpizza_v3;
if numberofadults = . then numberofadults = 0;
if numberofchildren = . then numberofchildren = 0;
if ppi = . then ppi = 0;
householdmembercount = numberofadults + numberofchildren;
run;
proc sort data = dis.ctdpizza_v4;
by mail_date;
run;
proc sql;
create table dis.ctdpizza_v5 as 
select mail_date, median(ppi) as c_median_ppi,median(householdmembercount) as c_medianMemCount from dis.ctdpizza_v4 group by mail_date;
quit;

/*findign threshold*/
data dis.threshold_v1;
merge dis.mtdpizza_v5(in = flag1) dis.ctdpizza_v5(in = flag2);
by mail_date;
grp1 = flag1;
grp2 = flag2;
run;


data dis.threshold_v2;
set dis.threshold_v1;
thresh_ppi = (median_ppi+c_median_ppi)/2;
tresh_mem_count = (medianMemCount+c_medianMemCount)/2;
run;


/*merging threahold*/


data dis.mtdpizza_v6;
merge dis.mtdpizza_v4 dis.threshold_v2;
by mail_date;

data dis.mtdpizza_v7;
set dis.mtdpizza_v6;
if thresh_ppi = . then delete;
run;


data dis.ctdpizza_v6;
merge dis.ctdpizza_v4 dis.threshold_v2;
by mail_date;

data dis.ctdpizza_v7;
set dis.ctdpizza_v6;
if thresh_ppi = . then delete;
run;
/*basketting*/
data dis.mtdpizza_v8;
set dis.mtdpizza_v7;
if (ppi <= thresh_ppi) and householdmembercount <= thresh_mem_count then basket =1;
else if (ppi <= thresh_ppi) and householdmembercount > thresh_mem_count then basket =2;
else if (ppi > thresh_ppi) and householdmembercount <= thresh_mem_count then basket =3;
else if (ppi > thresh_ppi) and householdmembercount > thresh_mem_count then basket =4;
run;


data dis.ctdpizza_v8;
set dis.ctdpizza_v7;
if (ppi <= thresh_ppi) and householdmembercount <= thresh_mem_count then basket =1;
else if (ppi <= thresh_ppi) and householdmembercount > thresh_mem_count then basket =2;
else if (ppi > thresh_ppi) and householdmembercount <= thresh_mem_count then basket =3;
else if (ppi > thresh_ppi) and householdmembercount > thresh_mem_count then basket =4;
run;

proc sql;
create table dis.M_baskets_v1 as
select mail_date,basket, avg(ppi) as basketavgppi, avg(householdmembercount)as basketavgmemcount from dis.mtdpizza_v8 group by mail_date,basket;
quit;

proc sql;
create table dis.C_baskets_v1 as
select mail_date,basket, avg(ppi) as basketavgppi, avg(householdmembercount)as basketavgmemcount from dis.ctdpizza_v8 group by mail_date,basket;
quit;

data dis.mtdpizza_v8;
set dis.mtdpizza_v8;
m_mailBasket = catt(put(mail_date,date9.),basket);
run;
data dis.ctdpizza_v8;
set dis.ctdpizza_v8;
c_mailBasket = catt(put(mail_date,date9.),basket);
run;


proc sql;
create table dis.mtdpizza_v9 as
select * from dis.mtdpizza_v8 where m_mailbasket in (select c_mailbasket from dis.ctdpizza_v8);
quit;


proc sql;
create table dis.ctdpizza_v9 as
select * from dis.ctdpizza_v8 where c_mailbasket in (select m_mailbasket from dis.mtdpizza_v8);
quit;


proc sql;
create table dis.ctdpizza_v10 as 
select * from dis.ctdpizza_v9 cv9 inner join mis.controlmail cm 
on cv9.addressid = cm.addressid and
cv9.mail_date = cm.controldate;
quit;

proc sql;
create table dis.ctdpizza_v11 as
select *
from dis.ctdpizza_v9 cv9
left join mis.controlmail cm 
on
  cv9.addressid = cm.addressid and
cv9.mail_date = cm.controldate
where cm.addressid is null;
quit; 

data dis.ctdpizza_v12;
set dis.ctdpizza_v11;
drop controldate maildate;
run;

proc sort data= dis.ctdpizza_v13;
by response;
quit;




proc surveyselect data=dis.ctdpizza_v13
         method=srs n= 8366
         seed=10000 out=dis.ctdpizza_stratifed_v2;
      strata response;
   run;



proc sort data= dis.mtdpizza_v9;
by response;
quit;


   

proc surveyselect data=dis.mtdpizza_v9
         method=srs n= 147935
         seed=10000 out=dis.mtdpizza_stratifed_v2;
      strata response;
   run;

ods graphics on;
proc logistic data= dis.ctdpizza_stratifed_v2 plots(MAXPOINTS=NONE)=(roc(id=obs) effect);
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDStratRegOutput_v1 predicted = pred predprobs=individual;
 score data= dis.ctdpizza_v13 out= dis.ctdpizza_scored_v1;
run;
ods graphics off;

/*MTD REGRESSION*/

ods graphics on;
proc logistic data= dis.mtdpizza_stratifed_v2 plots(MAXPOINTS=NONE)=(roc(id=obs) effect);
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb;
/*OUTPUT OUT= dis.MTDRegOutput_v1 predicted = pred predprobs=individual;
score data= dis.mtdpizza_v9 out= dis.mtdpizza_scored_v1;*/
run;
ods graphics off;


data dis.mtdpizza_scored_v2;
set dis.mtdpizza_scored_v1;
where p_1 is not null; 
run;


data dis.ctdpizza_scored_v2;
set dis.ctdpizza_scored_v1;
where p_1 is not null; 
run;


proc freq data=dis.CTDStratRegOutput_v1;
        table response*_INTO_ / out=dis.CellCounts;
        run;

proc freq data=dis.MTDRegOutput_v1;
        table response*_INTO_ / out=dis.CellCounts_M;
        run;


     /* data dis.CellCounts;
        set dis.CellCounts;
        Match=0;
        if Style=_INTO_ then Match=1;
        run;
      proc means data=dis.CellCounts mean;
        freq count;
        var Match;
        run;*/


/* Model 2 Approach */

data dis.CM_mergedStratifiedsamples;
set dis.mtdpizza_stratifed_v2(in=mail) dis.ctdpizza_stratifed_v2;
if mail=1 then M=1; else M=0;
run;


data dis.CM_mergedforReg;
set  dis.mtdpizza_v9 (in=mail) dis.ctdpizza_v13;
if mail=1 then M=1; else M=0;
run;

proc logistic data= dis.CM_mergedStratifiedsamples; /*  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user;
   model  response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1   
/*interaction with dummy M*/
M*occupancycount M*numberofchildren M*numberofadults M*heavy_internet_user M*ppi M*creditcard_holder M*maritalstatus  M*lhi_cooking M*lhi_general_sports M*avgrecency_score
M*avgdrinkord M*avgdessertcount M*avgchickenord M*avgallbreadcount M*avgorderamount M*AvgBfr_3month_menuAmt M*AvgBfr_3month_transaction  M*Avgmenuamount
M*Avgmarketingspend M*int1   

/ expb;
OUTPUT OUT= dis.CM_mergedforReg_out predicted = pred predprobs=individual;
score data= dis.CM_mergedforReg out= dis.CM_mergedforReg_scored_v1;
run;

proc logistic data= dis.CM_mergedStratifiedsamples; /*  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user;
   model  response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1  
/*interaction without dummy M*/
/ expb;
OUTPUT OUT= dis.CM_mergedforReg_outwithoutdummy predicted = pred predprobs=individual;
score data= dis.CM_mergedforReg out= dis.Test_withoutdummy;
run;


proc sql;
create table dis.CheckPickers as 
select sf.addressid, cadre, rank, sum(coupon) as couponsused from dis.score_features_v4 sf inner join dis.Log_Trans_V1 LT on 
sf.addressid = LT.addressid
group by sf.addressid, cadre,rank ;
quit;

proc sql;
select cadre, avg(couponsused) from dis.CheckPickers
group by cadre;
quit;

data dis.testC;
set dis.ctdpizza_stratifed_v2;
M=1;
run;

proc logistic data= dis.testC plots(MAXPOINTS=NONE)=(roc(id=obs) effect);
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 M

M*occupancycount M*numberofchildren M*numberofadults M*heavy_internet_user M*ppi M*creditcard_holder M*maritalstatus  M*lhi_cooking M*lhi_general_sports M*avgrecency_score
M*avgdrinkord M*avgdessertcount M*avgchickenord M*avgallbreadcount M*avgorderamount M*AvgBfr_3month_menuAmt M*AvgBfr_3month_transaction  M*Avgmenuamount
M*Avgmarketingspend M*int1 

/ expb;
/*OUTPUT OUT= dis.CTDStratRegOutput_v1 predicted = pred predprobs=individual;
 score data= dis.ctdpizza_v13 out= dis.ctdpizza_scored_v1;*/
run;


data dis.testM;
set dis.mtdpizza_stratifed_v2;
M=1;
run;

proc logistic data= dis.testM plots(MAXPOINTS=NONE)=(roc(id=obs) effect);
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 M
/*
M*occupancycount M*numberofchildren M*numberofadults M*heavy_internet_user M*ppi M*creditcard_holder M*maritalstatus  M*lhi_cooking M*lhi_general_sports M*avgrecency_score
M*avgdrinkord M*avgdessertcount M*avgchickenord M*avgallbreadcount M*avgorderamount M*AvgBfr_3month_menuAmt M*AvgBfr_3month_transaction  M*Avgmenuamount
M*Avgmarketingspend M*int1 */

/ expb;
/*OUTPUT OUT= dis.CTDStratRegOutput_v1 predicted = pred predprobs=individual;
 score data= dis.ctdpizza_v13 out= dis.ctdpizza_scored_v1;*/
run;

proc export data = dis.prob_stat_v3
outfile = "C:\Users\axv163930\Documents\ProbStat3"
dbms=xlsx replace;
run;


proc sql;


/*Incremental model*/

data dis.c1tdprob_v0;
set dis.CM_mergedforReg_scored_v1;
where P_1 is not null;
run;

proc sql;
create table dis.c1tdProb_v1 as
select c_mailbasket, avg(P_1) as basket_Prob from dis.c1tdprob_v0 
where m=0 
group by c_mailbasket; 
quit;

proc sql;
create table dis.m1tdProb_v1 as
select addressid,m_mailbasket, avg(P_1)as Addressid_maildt_Prob,custtype,zip5 from dis.c1tdprob_v0
where m=1
group by addressid,m_mailbasket;
quit;

data dis.c1tdProb_v1;
set dis.c1tdProb_v1;
rename c_mailbasket = m_mailbasket;
run;

proc sort data = dis.m1tdProb_v1;
by m_mailbasket;
run;
proc sort data = dis.c1tdProb_v1;
by m_mailbasket;
run;

data dis.mc1tdProb_v2;
merge dis.m1tdProb_v1(in  = i1) dis.c1tdProb_v1 (in = i2);
mtd = i1;
ctd = i2;
by m_mailbasket;
run;

data dis.mc1tdProb_v3;
set dis.mc1tdProb_v2;
Incremental_Prob= Addressid_maildt_Prob-basket_Prob;
run;

proc sql;
select p_1 from dis.mtdpizza_scored_v1 where addressid = 720378;
quit;

proc sql;
select p_1 from dis.cm_mergedforreg_scored_v1 where addressid = 720378;
quit;
******************************************************************************************************************************

libname dis '';

data iter2.Mailed;
set iter2.Mailed;
if  AddressId = . then delete;
run;
/*data  iter2.transactions4;
set iter2.transactions4;
drop outtime;
run;*/

/*appending all transaction files into one Log_Trans_V1*/
data iter2.Log_Trans_V1(keep=addressid zip5 storenum dateofOrder ordertype OrderAmount menuamount domcustid allbreadcount wkday coupon mealperiod AllBreadCount ChickenOrd DrinkOrd DessertCount);
set iter2.transactions1 iter2.transactions2 iter2.transactions3 iter2.transactions4;
if couponcode = ' ' then coupon = 0 ; else coupon = 1;
t=1;
by addressid;
run;
data iter2.Log_Trans_V1;
set iter2.Log_Trans_V1;
if addressid = . then delete;
run;


/* sorting mailed and Log_Trans_V1 file on addressedid */
proc sort data = iter2.Mailed;
by AddressId;
run;
proc sort data = iter2.Log_Trans_V1;
by AddressId;
run;


/*merging mailed and transactions */
data iter2.log_M_T_v1;
merge iter2.Mailed iter2.Log_Trans_V1 ;
by addressid;
/*keep addressid StoreNum MarketingSpend inhomedate CustType zip5 OrderAmount DateOfOrder mailed Mail_date;*/
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
run;

/* marking missing date of order -1 varname- NDOO ; and customers in mailes with no transactions as 1 varname - NTRANS*/
data iter2.log_M_T_v2;
set iter2.log_M_T_v1;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;



/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 3533941 */
data dis.mtpizza1;
set iter2.log_M_T_v2;
if NDOO = 1 and NTRANS = 0 then delete;
run;

/*Historic transactions*/
data dis.mtpizza2;
set dis.mtpizza1;
drop NDOO  NTRANS;
where dateoforder < mail_date;
run;

data dis.mtpizza3;
set dis.mtpizza2;
if custtype = 'New' or custtype = 'New OL' or custtype = 'New NOL' then Recency_Score = 6;

else if custtype = 'MVP' or custtype = 'MVP OL' or custtype = 'MVP NOL' 
or custtype = 'Frequent' or custtype = 'Frequent OL' or custtype = 'Frequent NOL' 
or custtype = 'Rejuvenated' or custtype = 'Rejuvenated OL' or custtype = 'Rejuvenated NOL' then Recency_Score = 5;

else if custtype = 'At Risk' or custtype = 'At Risk OL' or custtype = 'At Risk NOL' then Recency_Score = 4;
else if custtype = 'High Risk' or custtype = 'High Risk OL' or custtype = 'High Risk NOL' then Recency_Score = 3;
else if custtype = 'Max Risk' or custtype = 'Max Risk OL' or custtype = 'Max Risk NOL' then Recency_Score = 2;
else if custtype = 'Lost' or custtype = 'Lost OL' or custtype = 'Lost NOL' then Recency_Score = 1;
else Recency_Score = 3.5;
run;


data dis.mtpizza4;
set dis.mtpizza3;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_transaction = 0;
else Bfr_3month_transaction = 1;
drop inhomedate creative piecetype;
run;



/*lift calculation*/




data dis.mtlift1;
set dis.mtpizza1;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then after_mail = 1; else after_mail = 0;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then before_mail = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;

/*data  iter2.M_t_merge_4; 
set iter2.M_t_merge_4;
if mail_date=. then delete; 
run; */
 

proc sort data =  dis.mtlift1; 
by AddressId PullId;  
run;
 
/* To group AddressId and mail date and lots of other things*/   
data dis.mtlift2;
set dis.mtlift1; 
/*keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;*/
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;

data dis.mtlift3;
set  dis.mtlift2;
lift = avg_menu_amt_aft - avg_menu_amt_bfr;
if lift > 0 then response =1; else response =0;
keep addressid pullid mail_date avg_menu_amt_Bfr avg_menu_amt_aft lift response;
run;

proc sort data = dis.mtpizza4;  
by addressid pullid;
run;

proc sort data = dis.mtlift3;
by addressid pullid;
run;

data dis.mtpizza5;
merge dis.mtpizza4  dis.mtlift3;
by addressid pullid;
run;
proc sort data = dis.mtpizza5;
by addressid pullid;
quit;
/* In  */

/*data dis.mtlift1;
set dis.mtpizza1;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then purchase = 1;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then purchase = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;
 */
data dis.mtpizza6;
set dis.mtpizza5;
drop ordertype wkday mealperiod coupon;
run;

data dis.mtpizza7;
set dis.mtpizza6;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_menuAmt = 0;
else Bfr_3month_menuAmt = MenuAmount;
run; 

data dis.mtpizza8;
set dis.mtpizza7;
if marketingspend = . then marketingspend =0;
if menuamount = . then menuamount =0;
if Bfr_3month_transaction = . then Bfr_3month_transaction =0;
if orderamount = . then orderamount =0;
if menuamount = . then menuamount =0;
if allbreadcount = . then allbreadcount =0;
if chickenord = . then chickenord =0;
if dessertcount = . then dessertcount =0;
if drinkord = . then drinkord =0;
if recency_score =. then recency_score = 0;
run;
/*aggregating transation variables*/

data dis.mtpizza9;
set dis.mtpizza8;
retain count s_marketingspend s_menuamount s_Bfr_3month_transaction s_Bfr_3month_menuAmt s_orderamount s_menuamount s_allbreadcount s_chickenord s_dessertcount
s_drinkord s_recency_score;
by addressid pullid;
if first.pullid then 
do
count = 0;
s_marketingspend =0;
s_menuamount =0;
s_Bfr_3month_transaction =0;
s_Bfr_3month_menuAmt =0;
s_orderamount =0;
s_menuamount =0;
s_allbreadcount =0;
s_chickenord =0;
s_dessertcount=0;
s_drinkord =0;
s_recency_score=0;
end;
count = count + 1;
keep addressid pullid storenum custtype zip5 mail_date Avgmarketingspend Avgmenuamount AvgBfr_3month_transaction AvgBfr_3month_menuAmt Avgorderamount Avgmenuamount Avgallbreadcount Avgchickenord Avgdessertcount
Avgdrinkord Avgrecency_score lift response;

s_marketingspend = s_marketingspend + marketingspend;
s_menuamount = s_menuamount + menuamount;
s_Bfr_3month_transaction = s_Bfr_3month_transaction + Bfr_3month_transaction;
s_Bfr_3month_menuAmt = s_Bfr_3month_menuAmt + Bfr_3month_menuAmt;
s_orderamount = s_orderamount + orderamount;
s_allbreadcount = s_allbreadcount + allbreadcount;
s_drinkord = s_drinkord + drinkord;
s_recency_score = s_recency_score + recency_score;

if last.pullid then
do
Avgmarketingspend = s_marketingspend / count;
AvgBfr_3month_transaction = s_Bfr_3month_transaction/count;
AvgBfr_3month_menuAmt = s_Bfr_3month_menuAmt/ count;
Avgorderamount = s_orderamount / count;
Avgmenuamount = s_menuamount / count;
Avgallbreadcount = s_allbreadcount / count;
Avgchickenord = s_chickenord / count;
Avgdessertcount = s_dessertcount / count;
Avgdrinkord = s_drinkord / count;
Avgrecency_score = s_recency_score / count;
output;
end; 
run;

data dis.mtdpizza_v1;
merge dis.mtpizza9 (in = mt) dis.log_demog_v1 (in = demog);
by addressid;
mt1 = mt;
demog1 = demog;
run;

data dis.mtdpizza_v2;
set dis.mtdpizza_v1;
if mt1= 0 and demog1 =1 then delete;
run;

libname dis '';

proc sql;
select * from dis.mtdpizza_v2 where pullid = 43001 and addressid = 10474191;
quit;


proc corr data=dis.mtdpizza_v2;

VAR Avgmarketingspend 
Avgmenuamount
AvgBfr_3month_transaction 
AvgBfr_3month_menuAmt 
Avgorderamount

Avgallbreadcount 
Avgchickenord 
Avgdessertcount
Avgdrinkord 
Avgrecency_score
occupancycount

numberofadults
numberofchildren
heavy_internet_user
lhi_cooking
lhi_general_sports
ppi
BY response;
run;

PROC MEANS data=dis.mtdpizza_v2 N NMISS MEAN STDDEV MIN P25 MEDIAN P75 MAX;
	VAR Avgmarketingspend 
Avgmenuamount
AvgBfr_3month_transaction 
AvgBfr_3month_menuAmt 
Avgorderamount
Avgmenuamount
Avgallbreadcount 
Avgchickenord 
Avgdessertcount
Avgdrinkord 
Avgrecency_score
occupancycount
creditcardholder
maritalstatus
numberofadults
numberofchildren
heavy_internet_user
lhi_cooking
lhi_general_sports
ppi
response;
	TITLE 'Summary Statistics';
	
RUN;
data dis.mtdpizza_v3;
set dis.mtdpizza_v2;
int1 = ppi*numberofchildren;
int2 = ppi*lhi_cooking;
run;
/*
PROC REG DATA = dis.mtdpizza_v2;
     
	MODEL response = ppi numberofchildren lhi_cooking int1 int2;
	
	BY addressid pullid;
	
RUN;
*/
ods graphics on;
proc logistic data= dis.mtdpizza_v3 plots;*(MAXPOINTS=NONE)=(roc(id=obs) effect); 
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb ;
run;
ods graphics off;


/*Control*/
 
proc sort data = dis.control_master;
by addressid;
run;


proc sort data = dis.Log_Trans_V1;
by addressid;
run;

data dis.ctpizza_v1; 
merge dis.control_master(in = control_master) dis.Log_Trans_V1(in =Log_Trans_V1 );
by addressid ;
control_masterInd =control_master;
Log_Trans_V1Ind =Log_Trans_V1;
run;

data dis.ctpizza_v2;
set dis.ctpizza_v1; 
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
if control_masterInd = 0 and Log_Trans_V1Ind = 1 then delete;
run;

data dis.ctpizza_v3;
set dis.ctpizza_v2;
if pullid = '.' then delete;
if  dateoforder= '.' then NDOO = 1 ;else NDOO = 0;
if  dateoforder = '.' and zip5 = '.' and menuamount = '.' and orderamount = '.' then NTRANS= 1 ; else NTRANS = 0;
run;

/*sorting out all those observations/addressid with no order date (NDOO=1) only (NTRANS =0); count = 3533941 */
data dis.ctpizza_v4;
set dis.ctpizza_v3;
if NDOO = 1 and NTRANS = 0 then delete;
run;


/*Historic transactions*/
data dis.ctpizza_v5;
set dis.ctpizza_v4;
drop NDOO  NTRANS;
where dateoforder < mail_date;
run;

data dis.ctpizza_v6;
set dis.ctpizza_v5;
if custtype = 'New' or custtype = 'New OL' or custtype = 'New NOL' then Recency_Score = 6;

else if custtype = 'MVP' or custtype = 'MVP OL' or custtype = 'MVP NOL' 
or custtype = 'Frequent' or custtype = 'Frequent OL' or custtype = 'Frequent NOL' 
or custtype = 'Rejuvenated' or custtype = 'Rejuvenated OL' or custtype = 'Rejuvenated NOL' then Recency_Score = 5;

else if custtype = 'At Risk' or custtype = 'At Risk OL' or custtype = 'At Risk NOL' then Recency_Score = 4;
else if custtype = 'High Risk' or custtype = 'High Risk OL' or custtype = 'High Risk NOL' then Recency_Score = 3;
else if custtype = 'Max Risk' or custtype = 'Max Risk OL' or custtype = 'Max Risk NOL' then Recency_Score = 2;
else if custtype = 'Lost' or custtype = 'Lost OL' or custtype = 'Lost NOL' then Recency_Score = 1;
else Recency_Score = 3.5;
run;


data dis.ctpizza_v7;
set dis.ctpizza_v6;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_transaction = 0;
else Bfr_3month_transaction = 1;
drop inhomedate creative piecetype;
run;

proc sql;
select count(*) from  
dis.ctlift1 where after_mail =1 ;
quit;
/*lift calculation*/

data dis.ctlift1;
set dis.ctpizza_v4;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then after_mail = 1; else after_mail = 0;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then before_mail = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
run;

/*data  iter2.M_t_merge_4; 
set iter2.M_t_merge_4;
if mail_date=. then delete; 
run; */
 

proc sort data =  dis.ctlift1; 
by AddressId PullId;  
run;
 
/* To group AddressId and mail date and lots of other things*/   
data dis.ctlift2;
set dis.ctlift1; 
/*keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;*/
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;

data dis.ctlift3;
set  dis.ctlift2;
lift = avg_menu_amt_aft - avg_menu_amt_bfr;
if lift > 0 then response =1; else response =0;
keep addressid mail_date pullid avg_menu_amt_Bfr avg_menu_amt_aft lift response;
run;
/*endol*/
proc sort data = dis.ctpizza_v7;  
by addressid pullid;
run;

proc sort data = dis.ctlift3;
by addressid pullid;
run;

data dis.ctpizza_v8;
merge dis.ctpizza_v7  dis.ctlift3;
by addressid pullid;
run;

 
data dis.ctpizza_v9;
set dis.ctpizza_v8;
drop ordertype wkday mealperiod coupon;
run;

data dis.ctpizza_v10;
set dis.ctpizza_v9;
if ((mail_date-dateoforder) > 90) or (dateoforder = .) then Bfr_3month_menuAmt = 0;
else Bfr_3month_menuAmt = MenuAmount;
run; 

data dis.ctpizza_v11;
set dis.ctpizza_v10;
if marketingspend = . then marketingspend =0;
if menuamount = . then menuamount =0;
if Bfr_3month_transaction = . then Bfr_3month_transaction =0;
if orderamount = . then orderamount =0;
if menuamount = . then menuamount =0;
if allbreadcount = . then allbreadcount =0;
if chickenord = . then chickenord =0;
if dessertcount = . then dessertcount =0;
if drinkord = . then drinkord =0;
if recency_score =. then recency_score = 0;
run;
/*aggregating transation variables*/

data dis.ctpizza_v12;
set dis.ctpizza_v11;
retain count s_marketingspend s_menuamount s_Bfr_3month_transaction s_Bfr_3month_menuAmt s_orderamount s_menuamount s_allbreadcount s_chickenord s_dessertcount
s_drinkord s_recency_score;
by addressid pullid;
if first.pullid then 
do
count = 0;
s_marketingspend =0;
s_menuamount =0;
s_Bfr_3month_transaction =0;
s_Bfr_3month_menuAmt =0;
s_orderamount =0;
s_menuamount =0;
s_allbreadcount =0;
s_chickenord =0;
s_dessertcount=0;
s_drinkord =0;
s_recency_score=0;
end;
count = count + 1;
keep addressid pullid storenum custtype zip5 mail_date Avgmarketingspend Avgmenuamount AvgBfr_3month_transaction AvgBfr_3month_menuAmt Avgorderamount Avgmenuamount Avgallbreadcount Avgchickenord Avgdessertcount
Avgdrinkord Avgrecency_score lift response;

s_marketingspend = s_marketingspend + marketingspend;
s_menuamount = s_menuamount + menuamount;
s_Bfr_3month_transaction = s_Bfr_3month_transaction + Bfr_3month_transaction;
s_Bfr_3month_menuAmt = s_Bfr_3month_menuAmt + Bfr_3month_menuAmt;
s_orderamount = s_orderamount + orderamount;
s_allbreadcount = s_allbreadcount + allbreadcount;
s_drinkord = s_drinkord + drinkord;
s_recency_score = s_recency_score + recency_score;

if last.pullid then
do
Avgmarketingspend = s_marketingspend / count;
AvgBfr_3month_transaction = s_Bfr_3month_transaction/count;
AvgBfr_3month_menuAmt = s_Bfr_3month_menuAmt/ count;
Avgorderamount = s_orderamount / count;
Avgmenuamount = s_menuamount / count;
Avgallbreadcount = s_allbreadcount / count;
Avgchickenord = s_chickenord / count;
Avgdessertcount = s_dessertcount / count;
Avgdrinkord = s_drinkord / count;
Avgrecency_score = s_recency_score / count;
output;
end; 
run;

data dis.ctdpizza_v1;
merge dis.ctpizza_v12 (in = ct) dis.log_demog_v1 (in = demog);
by addressid;
ct1 = ct;
demog1 = demog;
run;

data dis.ctdpizza_v2;
set dis.ctdpizza_v1;
if ct1= 0 and demog1 =1 then delete;
run;

data dis.ctdpizza_v3;
set dis.ctdpizza_v2;
int1 = ppi*numberofchildren;
*int2 = ppi*lhi_cooking;
run;

ods graphics on;
proc logistic data= dis.ctdpizza_v4; /* plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDregOutput predicted = pred;
run;
ods graphics off;
 
proc sort data = dis.CTDregOutput;
by descending pred;
quit;

proc sort data = dis.ctdpizza_v4;
by response;
quit;


proc surveyselect data=dis.ctdpizza_v4
         method=srs n= 1809
         seed=10000 out=dis.ctdpizza_stratifed_v1;
      strata response;
   run;



ods graphics on;
proc logistic data= dis.ctdpizza_stratifed_v1  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); 
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDStratRegOutput predicted = pred;
run;
ods graphics off;

/*MTD REGRESSION*/

ods graphics on;
proc logistic data= dis.mtdpizza_v3; /*  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb ;
OUTPUT OUT= dis.MTDRegOutput predicted = pred;
run;
ods graphics off;

data dis.mtdpizza_v4;
set dis.mtdpizza_v3;
if custtype = ' ' then delete;
run;

proc sort data = dis.MTDregOutput;
by descending pred;
quit;

proc sort data = dis.mtdpizza_v4;
by response;
quit;



proc surveyselect data=dis.mtdpizza_v4
         method=srs n= 75406
         seed=10000 out=dis.mtdpizza_stratifed_v1;
      strata response;
   run;


/* baset creation */
   data dis.mtdpizza_v4;
set dis.mtdpizza_v3;
if numberofadults = . then numberofadults = 0;
if numberofchildren = . then numberofchildren = 0;
if ppi = . then ppi = 0;
householdmembercount = numberofadults + numberofchildren;
run;
proc sort data = dis.mtdpizza_v4;
by mail_date;
run;
proc sql;
create table dis.mtdpizza_v5 as 
select mail_date, median(ppi) as median_ppi,median(householdmembercount) as medianMemCount from dis.mtdpizza_v4 group by mail_date;
quit;

/* basket creation for control*/
 data dis.ctdpizza_v4;
set dis.ctdpizza_v3;
if numberofadults = . then numberofadults = 0;
if numberofchildren = . then numberofchildren = 0;
if ppi = . then ppi = 0;
householdmembercount = numberofadults + numberofchildren;
run;
proc sort data = dis.ctdpizza_v4;
by mail_date;
run;
proc sql;
create table dis.ctdpizza_v5 as 
select mail_date, median(ppi) as c_median_ppi,median(householdmembercount) as c_medianMemCount from dis.ctdpizza_v4 group by mail_date;
quit;

/*findign threshold*/
data dis.threshold_v1;
merge dis.mtdpizza_v5(in = flag1) dis.ctdpizza_v5(in = flag2);
by mail_date;
grp1 = flag1;
grp2 = flag2;
run;


data dis.threshold_v2;
set dis.threshold_v1;
thresh_ppi = (median_ppi+c_median_ppi)/2;
tresh_mem_count = (medianMemCount+c_medianMemCount)/2;
run;


/*merging threahold*/


data dis.mtdpizza_v6;
merge dis.mtdpizza_v4 dis.threshold_v2;
by mail_date;

data dis.mtdpizza_v7;
set dis.mtdpizza_v6;
if thresh_ppi = . then delete;
run;


data dis.ctdpizza_v6;
merge dis.ctdpizza_v4 dis.threshold_v2;
by mail_date;

data dis.ctdpizza_v7;
set dis.ctdpizza_v6;
if thresh_ppi = . then delete;
run;
/*basketting*/
data dis.mtdpizza_v8;
set dis.mtdpizza_v7;
if (ppi <= thresh_ppi) and householdmembercount <= thresh_mem_count then basket =1;
else if (ppi <= thresh_ppi) and householdmembercount > thresh_mem_count then basket =2;
else if (ppi > thresh_ppi) and householdmembercount <= thresh_mem_count then basket =3;
else if (ppi > thresh_ppi) and householdmembercount > thresh_mem_count then basket =4;
run;


data dis.ctdpizza_v8;
set dis.ctdpizza_v7;
if (ppi <= thresh_ppi) and householdmembercount <= thresh_mem_count then basket =1;
else if (ppi <= thresh_ppi) and householdmembercount > thresh_mem_count then basket =2;
else if (ppi > thresh_ppi) and householdmembercount <= thresh_mem_count then basket =3;
else if (ppi > thresh_ppi) and householdmembercount > thresh_mem_count then basket =4;
run;

proc sql;
create table dis.M_baskets_v1 as
select mail_date,basket, avg(ppi) as basketavgppi, avg(householdmembercount)as basketavgmemcount from dis.mtdpizza_v8 group by mail_date,basket;
quit;

proc sql;
create table dis.C_baskets_v1 as
select mail_date,basket, avg(ppi) as basketavgppi, avg(householdmembercount)as basketavgmemcount from dis.ctdpizza_v8 group by mail_date,basket;
quit;

data dis.mtdpizza_v8;
set dis.mtdpizza_v8;
m_mailBasket = catt(put(mail_date,date9.),basket);
run;
data dis.ctdpizza_v8;
set dis.ctdpizza_v8;
c_mailBasket = catt(put(mail_date,date9.),basket);
run;


proc sql;
create table dis.mtdpizza_v9 as
select * from dis.mtdpizza_v8 where m_mailbasket in (select c_mailbasket from dis.ctdpizza_v8);
quit;


proc sql;
create table dis.ctdpizza_v9 as
select * from dis.ctdpizza_v8 where c_mailbasket in (select m_mailbasket from dis.mtdpizza_v8);
quit;


proc sql;
create table dis.ctdpizza_v10 as 
select * from dis.ctdpizza_v9 cv9 inner join mis.controlmail cm 
on cv9.addressid = cm.addressid and
cv9.mail_date = cm.controldate;
quit;

proc sql;
create table dis.ctdpizza_v11 as
select *
from dis.ctdpizza_v9 cv9
left join mis.controlmail cm 
on
  cv9.addressid = cm.addressid and
cv9.mail_date = cm.controldate
where cm.addressid is null;
quit; 

data dis.ctdpizza_v12;
set dis.ctdpizza_v11;
drop controldate maildate;
run;

proc sort data= dis.ctdpizza_v13;
by response;
quit;




proc surveyselect data=dis.ctdpizza_v13
         method=srs n= 8366
         seed=10000 out=dis.ctdpizza_stratifed_v2;
      strata response;
   run;



proc sort data= dis.mtdpizza_v9;
by response;
quit;


   

proc surveyselect data=dis.mtdpizza_v9
         method=srs n= 147935
         seed=10000 out=dis.mtdpizza_stratifed_v2;
      strata response;
   run;

ods graphics on;
proc logistic data= dis.ctdpizza_stratifed_v2;/* plots(MAXPOINTS=NONE)=(roc(id=obs) effect);*/
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event ='1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 / expb;
OUTPUT OUT= dis.CTDStratRegOutput_v1 predicted = pred predprobs=individual;
 score data= dis.ctdpizza_v13 out= dis.ctdpizza_scored_v1;
run;
ods graphics off;

/*MTD REGRESSION*/

ods graphics on;
proc logistic data= dis.mtdpizza_stratifed_v2; /*  plots(MAXPOINTS=NONE)=(roc(id=obs) effect); */
   class   creditcard_holder maritalstatus lhi_general_sports lhi_cooking heavy_internet_user  ;
   model  response (event = '1') =  occupancycount numberofchildren numberofadults heavy_internet_user ppi creditcard_holder maritalstatus  lhi_cooking lhi_general_sports avgrecency_score
avgdrinkord avgdessertcount avgchickenord avgallbreadcount avgorderamount AvgBfr_3month_menuAmt AvgBfr_3month_transaction  Avgmenuamount
Avgmarketingspend int1 int2/ expb;
OUTPUT OUT= dis.MTDRegOutput_v1 predicted = pred predprobs=individual;
score data= dis.mtdpizza_v9 out= dis.mtdpizza_scored_v1;
run;
ods graphics off;


data dis.mtdpizza_scored_v2;
set dis.mtdpizza_scored_v1;
where p_1 is not null; 
run;


data dis.ctdpizza_scored_v2;
set dis.ctdpizza_scored_v1;
where p_1 is not null; 
run;


proc freq data=dis.CTDStratRegOutput_v1;
        table response*_INTO_ / out=dis.CellCounts;
        run;

proc freq data=dis.MTDRegOutput_v1;
        table response*_INTO_ / out=dis.CellCounts_M;
        run;


     /* data dis.CellCounts;
        set dis.CellCounts;
        Match=0;
        if Style=_INTO_ then Match=1;
        run;
      proc means data=dis.CellCounts mean;
        freq count;
        var Match;
        run;*/

		/*SCORING*/
libname Dis '';

		proc sql;
create table dis.Mtdpizza_scored_v3 as
select * from dis.Mtdpizza_scored_v2 where m_mailbasket in (select c_mailbasket from dis.ctdpizza_scored_v2);
quit;


proc sql;
create table dis.ctdpizza_scored_v3 as
select * from dis.ctdpizza_scored_v2 where c_mailbasket in (select m_mailbasket from dis.mtdpizza_scored_v2);
quit;


proc sql;
create table dis.ctdProb_v1 as
select c_mailbasket, avg(P_1) as basket_Prob from dis.ctdpizza_scored_v3 group by c_mailbasket;
quit;

proc sql;
create table dis.mtdProb_v1 as
select addressid,m_mailbasket, avg(P_1)as Addressid_maildt_Prob,custtype,zip5 from dis.Mtdpizza_scored_v3 group by addressid,m_mailbasket;
quit;

data dis.ctdProb_v1;
set dis.ctdProb_v1;
rename c_mailbasket = m_mailbasket;
run;

proc sort data = dis.mtdProb_v1;

by m_mailbasket;
proc sort data = dis.ctdProb_v1;
by m_mailbasket;

data dis.mctdProb_v2;
merge dis.mtdProb_v1(in  = i1) dis.ctdProb_v1 (in = i2);
mtd = i1;
ctd = i2;
by m_mailbasket;
run;
data dis.mctdProb_v3;
set dis.mctdProb_v2;
Incremental_Prob= Addressid_maildt_Prob-basket_Prob;
run;


proc sql;
create table dis.mctdProb_v4 as
select addressid, avg(Incremental_Prob)as Prob_Of_Response from dis.mctdProb_v3 group by addressid;
quit;

proc sort data = dis.mctdProb_v4;
by descending Prob_Of_Response;
run;

data dis.AddressId_Scored;
set dis.mctdProb_v4;
Rank = _n_;
run;

/*Scoring*/



proc sql;
create table dis.custype as
select addressid , custtype from iter2.mailed;
quit;
proc sort data = dis.custype;
by addressid;
run;

libname iter2 '';
proc sql;
create table dis.zip1 as
select addressid , zip5 from iter2.demographics_big;
quit;
proc sort data = dis.zip;
by addressid;
run;

data dis.cz;
merge dis.custype(in = i1) dis.zip(in = i2);
c= i1;
z=i2;
by addressid;


data dis.custypezip2;
set dis.custypezip;
laddressid = lag(addressid);
if laddressid = addressid then delete;
run;

/* TODAY */
DATA DIS.aMAILdEMO;
merge iter2.mailed(in = i1) iter2.Demographics_big(in = i2);
m = i1;
d = i2;
by addressid;
run;

data dis.amaildemo2;
set dis.amaildemo;
if m = 0 and d = 1 then delete;
run;

data dis.amaildemo3;
set dis.amaildemo2;
ladd = lag(addressid);
if ladd = addressid then delete;
run;
proc sort data = dis.Addressid_Scored_2;
by addressid;
run;
proc sort data = dis.amaildemo3;
by addressid;
run;
data dis.Addressid_Scored_4;
merge dis.Addressid_Scored_2(in = a1) dis.amaildemo3 (in = a2);
by addressid;
a = a1;
b=a2;
if a=0 and b = 1 then delete;
run;
/*data dis.addressid_scored_2;
set dis.addressid_scored;
run;
proc sort data = dis.addressid_scored_2;
by addressid;
run;
proc sort data = dis.mtdpizza_v9;
by addressid;
run;*/


data dis.score_features_v1;
 merge dis.addressid_scored_2(in = i1) dis.mtdpizza_v9(in = i2);
 s = i1;
 f = i2;
 by addressid;
run;

data dis.score_features_v2;
set dis.score_features_v1;
if s= 0 and f = 1 then delete;
run;
 

proc sql;

where custtype in ('At Risk' ,
'At Risk NOL',
'At Risk OL',
'Frequent', 
'Frequent NOL',
'Frequent OL', 
'High Risk NOL',
'High Risk OL', 
'Lost NOL', 
'Lost OL', 
'MVP NOL', 
'MVP OL', 
'Max Risk NOL', 
'Max Risk OL', 
'New NOL', 
'New OL', 
'Rejuvenated',
'Rejuvenated NOL', 
'Rejuvenated OL')
group by custtype;
quit;


libname iter2 '';

data mktsec.At_Risk mktsec.At_Risk_NOL mktsec.At_Risk_OL mktsec.Frequent mktsec.Frequent_NOL mktsec.Frequent_OL 
mktsec.High_Risk_NOL mktsec.High_Risk_OL mktsec.Lost_NOL mktsec.Lost_OL
mktsec.MVP_NOL mktsec.MVP_OL mktsec.Max_Risk_NOL mktsec.Max_Risk_OL mktsec.New_NOL mktsec.New_OL mktsec.Rejuvenated 
mktsec.Rejuvenated_NOL mktsec.Misc_Mark_sector mktsec.new mktsec.mvp mktsec.high_risk mktsec.Max_risk mktsec.lost
mktsec.Rejuvenated_OL;
set dis.score_features_v2;

if custtype='At Risk' then output mktsec.At_Risk;
else if custtype='At Risk NOL' then output mktsec.At_Risk_NOL;
else if custtype='At Risk OL' then output mktsec.At_Risk_OL;
else if custtype='Frequent' then output mktsec.Frequent;
else if custtype='Frequent NOL' then output mktsec.Frequent_NOL;
else if custtype='Frequent OL' then output mktsec.Frequent_OL ;
else if custtype='High Risk NOL' then output mktsec.High_Risk_NOL;
else if custtype='High Risk OL' then output mktsec.High_Risk_OL;
else if custtype='High Risk' then output mktsec.high_risk;
else if custtype='Lost NOL' then output mktsec.Lost_NOL;
else if custtype='Lost OL' then output mktsec.Lost_OL;
else if custtype='Lost' then output mktsec.lost;
else if custtype='MVP' then output mktsec.mvp;
else if custtype='MVP NOL' then output mktsec.MVP_NOL;
else if custtype='MVP OL' then output mktsec.MVP_OL;
else if custtype='Max Risk NOL' then output mktsec.Max_Risk_NOL;
else if custtype='Max Risk OL' then output mktsec.Max_Risk_OL;
else if custtype='Max Risk' then output mktsec.Max_risk;
else if custtype='New' then output mktsec.new;
else if custtype='New NOL' then output mktsec.New_NOL;
else if custtype='New OL' then output mktsec.New_OL;
else if custtype='Rejuvenated' then output mktsec.Rejuvenated;
else if custtype='Rejuvenated NOL' then output mktsec.Rejuvenated_NOL;
else if custtype='Rejuvenated OL' then output mktsec.Rejuvenated_OL;
else output mktsec.Misc_Mark_sector;
run;





proc sort data = mktsec.At_Risk;
by descending Prob_Of_Response;
run;
proc sort data = mktsec.At_Risk_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.At_Risk_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Frequent;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Frequent_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Frequent_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.High_Risk_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.High_Risk_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.high_risk;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Lost_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Lost_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.lost;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.mvp;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.MVP_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.MVP_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Max_Risk_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Max_Risk_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Max_risk;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.new;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.New_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.New_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Rejuvenated;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Rejuvenated_NOL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Rejuvenated_OL;
by descending Prob_Of_Response;
run;

proc sort data = mktsec.Misc_Mark_sector;
by descending Prob_Of_Response;
run;
Data mktsec.At_Risk2;
set mktsec.At_Risk ;
drop rank;
Ranking = _n_;
run;


Data mktsec.At_Risk_NOL2;
set mktsec.At_Risk_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.At_Risk_OL2;
set mktsec.At_Risk_OL;
drop rank;
Ranking = _n_;
run;
Data mktsec.Frequent2;
set mktsec.Frequent;
drop rank;
Ranking = _n_;
run;

Data mktsec.Frequent_NOL2;
set mktsec.Frequent_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Frequent_OL2;
set mktsec.Frequent_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.High_Risk_NOL2;
set mktsec.High_Risk_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.High_Risk_OL2;
set mktsec.High_Risk_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.high_risk2;
set mktsec.high_risk;
drop rank;
Ranking = _n_;
run;
Data mktsec.Lost_NOL2;
set mktsec.Lost_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Lost_OL2;
set mktsec.Lost_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.lost2;
set mktsec.lost;
drop rank;
Ranking = _n_;
run;

Data mktsec.mvp2;
set mktsec.mvp;
drop rank;
Ranking = _n_;
run;

Data mktsec.MVP_NOL2;
set mktsec.MVP_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.MVP_OL2;
set mktsec.MVP_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Max_Risk_NOL2;
set mktsec.Max_Risk_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Max_Risk_OL2;
set mktsec.Max_Risk_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Max_risk2;
set mktsec.Max_risk;
drop rank;
Ranking = _n_;
run;


Data mktsec.new2;
set mktsec.new;
drop rank;
Ranking = _n_;
run;

Data mktsec.New_NOL2;
set mktsec.New_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.New_OL2;
set mktsec.New_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Rejuvenated2;
set mktsec.Rejuvenated;
drop rank;
Ranking = _n_;
run;

Data mktsec.Rejuvenated_NOL2;
set mktsec.Rejuvenated_NOL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Rejuvenated_OL2;
set mktsec.Rejuvenated_OL;
drop rank;
Ranking = _n_;
run;

Data mktsec.Misc_Mark_sector2;
set mktsec.Misc_Mark_sector;
drop rank;
Ranking = _n_;
run;

data dis.score_features_v3;
set dis.score_features_v2;
where custtype in ('At Risk' ,
'At Risk NOL',
'At Risk OL',
'Frequent', 
'Frequent NOL',
'Frequent OL', 
'High Risk NOL',
'High Risk OL', 
'Lost NOL', 
'Lost OL', 
'MVP NOL', 
'MVP OL', 
'Max Risk NOL', 
'Max Risk OL', 
'New NOL', 
'New OL', 
'Rejuvenated',
'Rejuvenated NOL', 
'Rejuvenated OL');
run;

proc sql;
create table dis.prob_stat as
select custtype,median(Prob_Of_Response) as median_Prob_Response, max(Prob_Of_Response) as max_Prob_Response, 
min (Prob_Of_Response)as min_Prob_Response from dis.score_features_v3 group by custtype;
quit;
proc sort data = dis.score_features_v3;
by custtype;
run;

proc means noprint data = dis.score_features_v3;
var Prob_Of_response;
by custtype;
output out = dis.prob_stat_2
min (Prob_Of_response) = min_increment_ProbRes
median (Prob_Of_response) = median_increment_ProbRes
P75 (Prob_Of_response) = P75_increment_ProbRes
max (Prob_Of_response) = max_increment_ProbRes;
run;

data dis.prob_stat_v3;
set dis.prob_stat_2;
drop _type_ _freq_;
run;


proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response <= 0;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > 0 and Prob_Of_Response<=.10;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .10 and Prob_Of_Response <= .20;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .20 and Prob_Of_Response <=.30;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .30 and Prob_Of_Response<= .40;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .40 and Prob_Of_Response <= .50;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .50 and Prob_Of_Response <= .60;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .60 and Prob_Of_Response <= .70;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .70 and Prob_Of_Response <= .80;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .80 and Prob_Of_Response <= .90;
quit;
proc sql;
select count(addressid) from dis.addressid_scored_2 where Prob_Of_Response > .90 and Prob_Of_Response <= 1;
quit;

data dis.score_features_v4;
set dis.score_features_v3;

drop rank;
run;

/*data dis.addressid_scored_3;
set dis.addressid_scored_2;
if Prob_Of_Response > 0;
drop rank;
run;
*/

proc sort data = dis.score_features_v4;
by descending prob_Of_response;
run;


data dis.score_features_v4;
set dis.score_features_v4;
rank = _n_;
if rank >=1 and rank<= 45436 then cadre = "top40Percent";
else if rank >=45437 and rank<= 90873 then cadre = "Middle30Percent";
else if rank >=90874 and rank<= 151455 then cadre = "Bottom30Percent";
else cadre = "Trashers";
run;

proc sql;
select cadre, avg(ppi),avg(householdmembercount)from dis.score_features_v4 group by cadre;
quit;

proc sql;
create table dis.addressid_scored_custtype as
select cadre, custtype, count(custtype)from dis.Addressid_scored_4 group by cadre,custtype;
quit;

proc sql;
create table dis.addressid_scored_zip5 as
select cadre, zip5, count(zip5)from dis.Addressid_scored_4 group by cadre,zip5;
quit;


proc sort data = dis.addressid_scored_4;
by descending rank;
run;


data dis.addressid_scored_4;
set dis.addressid_scored_4;
if rank >=1 and rank<= 153352 then cadre = "top40Percent";
else if rank >=153353 and rank<= 306705 then cadre = "Middle30Percent";
else if rank >=306706 and rank<= 511176 then cadre = "Bottom30Percent";
else cadre = "Trashers";
run;


proc sql;
select count(addressid) from dis.addressid_scored_4 where prob_of_response <=0;
run;


data dis.zip1;
set dis.mctdProb_v3;
keep addressid zip5;
run;

data dis.zip1;
set dis.zip1;
if zip5 = . then delete;
lad = lag (addressid);
if addressid = lad then delete;
drop lad;
run;

proc sort data = dis.Addressid_scored_4;
by addressid;
run;


proc sort data = dis.zip1;
by addressid;
run;
data dis.zip2;
merge dis.Addressid_scored_4(in = m1) dis.zip1(in = m2);
p = m1;
q= m2;
by addressid;
if p=0 and q = 1 then delete;
if zip5 = . then delete;
run;

proc sql;
create table dis.zipTrashers as
select distinct(zip5) from dis.zip2 where cadre = "Trashers";
quit;
proc sql;
create table dis.zip3top40Percent as
select distinct(zip5) from dis.zip2 where cadre = "top40Percent";
quit;
proc sql;
create table dis.zip3Bottom30Perc as
select distinct(zip5) from dis.zip2 where cadre = "Bottom30Perc" ;
quit;
proc sql;
create table dis.zip3Middle30Perc as
select distinct(zip5) from dis.zip2 where cadre = "Middle30Perc";
quit;
proc export data = dis.zipTrashers 
outfile = "C:\Users\dxj160830\Documents\zipTrashers"
dbms = xlsx replace;
run;
proc export data = dis.zip3top40Percent 
outfile = "C:\Users\dxj160830\Documents\zip3top40Percent"
dbms = xlsx replace;
run;
proc export data = dis.zip3Bottom30Perc 
outfile = "C:\Users\dxj160830\Documents\zip3Bottom30Perc"
dbms = xlsx replace;
run;
proc export data = dis.zip3Middle30Perc 
outfile = "C:\Users\dxj160830\Documents\zip3Middle30Perc"
dbms = xlsx replace;
run;
goptions reset=all cback=white border htitle=12pt htext=10pt;  

 /* Create a data set containing ZIP codes. */
data myzip;
  input Zip;
  datalines;

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
**************************************************************************************************************************
/*Descriptive stats alone*/
/* Import mailed.csv into sastable from fourth merge */ 
Libname iter2 '';
PROC IMPORT OUT= iter2.MAILED 
            DATAFILE= "\\smpnas02\MKT6337\TheSuperlatives\DominoPizza\NewData\FourthMerge\mailed.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data iter2.Mailed;
set iter2.Mailed;
if  AddressId = . then delete;
run;

/* sorting mailed file on addressedid */
proc sort data = iter2.Mailed;
by AddressId;
run;

/*merging mailed and transactions */
data iter2.M_T_merge;
merge iter2.Mailed iter2.transactions ;
 by addressid;
keep addressid StoreNum MarketingSpend inhomedate CustType zip5 OrderAmount DateOfOrder mailed Mail_date;
Mail_date=datepart(inhomedate);
format Mail_date date9. ;
run;

/*Removing recordxs with emptgy order date in the transaction table*/
data iter2.M_T_merge_2;
set iter2.M_T_merge;
if dateoforder = . then delete;
run;

/*data iter2.M_T_merge_3;
set iter2.M_T_merge_2;
if dateoforder< mail_date then Order_Before = 1; else Order_before = 0;
if dateoforder> (mail_date + 30) then Order_after = 1; else Order_after = 0;
if dateoforder> mail_date and dateoforder< (mail_date + 31) then Order_within_30 = 1; else Order_within_30 = 0;
run;*/

proc sql;
select * from iter2.mailed where addressid= 925;
quit;

proc sql;
select * from iter2.transactions where addressid= 925;
quit;


proc sql;
select * from iter2.m_t_merge_4 where addressid= 925;
quit;
/* final merge is used */
data iter2.MT_Type1_v1;
set iter2.MT_finalmerge;
if dateoforder > mail_date and dateoforder < (mail_date + 31) then after_mail = 1; else after_mail = 0;
if dateoforder< mail_date and dateoforder > (mail_date - 31) then before_mail = 1; else before_mail = 0;
if MenuAmount = . then menuAmount =0;
drop x;
run;

/*data  iter2.M_t_merge_4; 
set iter2.M_t_merge_4;
if mail_date=. then delete; 
run; */
 

proc sort data =  iter2.MT_Type1_v1; 
by AddressId PullId;  
run;
 
/* To group AddressId and mail date and lots of other things*/   
data iter2.MT_Type1_v2;
set iter2.MT_Type1_v1; 
keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;     
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;

data iter2.test2;
set iter2.M_T_merge_5;
if count_aft = 0 and count_bfr = 0 and (avg_menu_amt_Bfr>0 or avg_menu_amt_aft >0);
run;

proc sort data =  iter2.M_T_merge_5; 
by AddressId PullId;  
run;

data iter2.test1;
set iter2.m_t_merge_4;
if addressid = 740;
run;

data iter2.test100;
set iter2.mt_type1_v2;
where count_aft > 0 and count_bfr > 0;
run;

/* deferred
data iter2.M_T_merge_6; 
set iter2.M_T_merge_5; 
if count_aft ne 0 and count_bfr ne 0 and (count_aft = count_bfr or count_aft > count_bfr) then 
has_responded = 1; 
else 
has_responded =0; 
run; 
 
proc sort data =iter2.M_T_merge_6;
by has_responded;

run; 
 
 proc sql;
 select count(*) from iter2.M_T_merge_6 where has_responded = 1 group by addressid;
 quit;

 data iter2.M_t_merge_7; 
 set iter2.M_T_merge_6; 
 if has_responded=1; 
 run;


  data iter2.test1; 
 set iter2.M_T_merge_4; 
 if MenuAmount=.; 
 run;

proc sort data= iter2.M_T_merge_8; 
by 
*/

proc sql;
create table iter2.mt_type1_v3 as
select *, (avg_menu_amt_aft - avg_menu_amt_bfr) as Lift from iter2.mt_type1_v2;
quit;

data iter2.mt_type1_v4;
set iter2.mt_type1_v3;
if lift > 0 then has_responded = 1;
else has_responded =0;
run;

data iter2.mt_type1_v5;
set iter2.mt_type1_v4;
if lift > 0;
run;

data iter2.mt_type1_v6;
set iter2.mt_type1_v5;
if Custtype not in ('At Risk' ,
'At Risk NOL',
'At Risk OL',
'Frequent', 
'Frequent NOL',
'Frequent OL', 
'High Risk NOL',
'High Risk OL', 
'Lost NOL', 
'Lost OL', 
'MVP NOL', 
'MVP OL', 
'Max Risk NOL', 
'Max Risk OL', 
'New NOL', 
'New OL', 
'Rejuvenated',
'Rejuvenated NOL', 
'Rejuvenated OL') then CustType = 'Others';
run;

proc sql;
select custtype, avg(lift) as AvgLift from iter2.mt_type1_v6
group by custtype
order by AvgLift;
quit;

proc export data = iter2.mt_type1_v6
outfile = "C:\Users\axv163930\Documents\lift_type1"
dbms=xlsx replace;
run;
/*

Recency */
libname iter2 '';

/* recency added to the Log_M_T_V5*/
data iter2.Log_M_T_V5;
set iter2.Log_M_T_V4;
if custtype = 'New' or custtype = 'New OL' or custtype = 'New NOL' then Recency_Score = 6;

else if custtype = 'MVP' or custtype = 'MVP OL' or custtype = 'MVP NOL' 
or custtype = 'Frequent' or custtype = 'Frequent OL' or custtype = 'Frequent NOL' 
or custtype = 'Rejuvenated' or custtype = 'Rejuvenated OL' or custtype = 'Rejuvenated NOL' then Recency_Score = 5;

else if custtype = 'At Risk' or custtype = 'At Risk OL' or custtype = 'At Risk NOL' then Recency_Score = 4;
else if custtype = 'High Risk' or custtype = 'High Risk OL' or custtype = 'High Risk NOL' then Recency_Score = 3;
else if custtype = 'Max Risk' or custtype = 'Max Risk OL' or custtype = 'Max Risk NOL' then Recency_Score = 2;
else if custtype = 'Lost' or custtype = 'Lost OL' or custtype = 'Lost NOL' then Recency_Score = 1;
else Recency_Score = 3.5;
run;


data iter2.log_mtd_v2;
set iter2.log_mtd_v1;
if (mail_date-dateoforder) > 90  then Bfr_3month_transaction = 0;
else Bfr_3month_transaction = 1;
run;


proc sort data= iter2.Log_Trans_V1;
by dateoforder;
run;



proc sort data= iter2.MT_finalmerge;
by dateoforder;
run;

/* To group AddressId and mail date and lots of other things*/   
data iter2.MT_Type1_v2;
set iter2.MT_Type1_v1; 
keep AddressId Mail_date zip5 CustType PullId count_bfr count_aft avg_menu_amt_Bfr avg_menu_amt_aft;     
retain count_aft count_bfr MenuAmt_Bef MenuAmt_Aft avg_menu_amt_Bfr avg_menu_amt_aft;
by AddressId PullId;  
if first.PullId then do  
count_aft =0; 
count_bfr=0;
MenuAmt_Bef=0;
MenuAmt_Aft=0;
avg_menu_amt_Bfr=0;
avg_menu_amt_aft=0;
end; 
count_aft= count_aft + after_mail; 
count_bfr= count_bfr + before_mail; 
if before_mail=1 then 
MenuAmt_Bef= MenuAmt_Bef + MenuAmount;
if after_mail=1 then
MenuAmt_Aft= MenuAmt_Aft + MenuAmount;
if last.PullId then do;
if count_bfr ne 0 then
avg_menu_amt_Bfr = MenuAmt_Bef/count_bfr;
if count_aft ne 0 then 
avg_menu_amt_aft = MenuAmt_Aft/count_aft;
output;  
end;
run;
libname dis '';



data dis.mtdpizza_v4;
set dis.mtdpizza_v3;
if numberofadults = . then numberofadults = 0;
if numberofchildren = . then numberofchildren = 0;
if ppi = . then ppi = 0;
householdmembercount = numberofadults + numberofchildren;
run;
proc sort data = dis.mtdpizza_v4;
by mail_date;
run;
proc sql;
create table dis.mtdpizza_v5 as 
select mail_date, median(ppi) as median_ppi,median(householdmembercount) as medianMemCount from dis.mtdpizza_v4 group by mail_date;
quit;
/* yet another way of calculating median 


proc means noprint data = dis.mtdpizza_v4  ;
var ppi householdmembercount;
by mail_date;
*where mail_date = 20460;
output out = dis.mtdpizza_v6
median (ppi householdmembercount ) = ppi_med householdmembercount_med;
run;
 data  dis.mtdpizza_v7;
merge dis.mtdpizza_v5 dis.mtdpizza_v6;
by mail_date;
run;
yet another way of calculating median */

/*proc sql ;
select count(*) from dis.mtdpizza_v4 where mail_date = .;
quit;*/


proc sql ;
select count(*) from Dis.mtlift3 where mail_date = .;
quit;
proc sql;
select * from dis.mtpizza1 where addressid in (117,182,199,214);quit;

libname dis '';


proc sql;
create table dis.Mtdpizza_scored_v3 as
select * from dis.Mtdpizza_scored_v2 where m_mailbasket in (select c_mailbasket from dis.ctdpizza_scored_v2);
quit;


proc sql;
create table dis.ctdpizza_scored_v3 as
select * from dis.ctdpizza_scored_v2 where c_mailbasket in (select m_mailbasket from dis.mtdpizza_scored_v2);
quit;


proc sql;
create table dis.ctdProb_v1 as
select c_mailbasket, avg(P_1) as basket_Prob from dis.ctdpizza_scored_v3 group by c_mailbasket;
quit;

proc sql;
create table dis.mtdProb_v1 as
select addressid,m_mailbasket, avg(P_1)as Addressid_maildt_Prob from dis.Mtdpizza_scored_v3 group by addressid,m_mailbasket;
quit;

data dis.ctdProb_v1;
set dis.ctdProb_v1;
rename c_mailbasket = m_mailbasket;
run;

proc sort data = dis.mtdProb_v1;

by m_mailbasket;
proc sort data = dis.ctdProb_v1;
by m_mailbasket;

data dis.mctdProb_v2;
merge dis.mtdProb_v1(in  = i1) dis.ctdProb_v1 (in = i2);
mtd = i1;
ctd = i2;
by m_mailbasket;
run;
data dis.mctdProb_v3;
set dis.mctdProb_v2;
Incremental_Prob= Addressid_maildt_Prob-basket_Prob;
run;


proc sql;
create table dis.mctdProb_v4 as
select addressid, avg(Incremental_Prob)as Prob_Of_Response from dis.mctdProb_v3 group by addressid;
quit;

proc sort data = dis.mctdProb_v4;
by descending Prob_Of_Response;
run;

data dis.AddressId_Scored;
set dis.mctdProb_v4;
Rank = _n_;
run;

proc sql;
select * from dis.mctdprob_v3 where addressid = 9680487;
quit;



