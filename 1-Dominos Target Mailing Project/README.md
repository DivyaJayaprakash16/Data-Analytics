# Dominos-Data-Analysis-Challenge
# Target Marketing - Predictive Analytics

Overall Scoring of Customers in Each Market Sector

a.	Overview: 
    Score all customers in each market sector (each market sector should be treated independently of the other market sectors) with    
    likelihood to respond to direct mail in each store 

b.	Problem/Challenge: 
    The scoring of a customer will have to be fluid with the movement of the address. 
    
    For example: If a customer is a Frequent customer and scores high among other frequent customers,
                 but then has ordering pattern changes,
                 the address could fall into the At Risk market sector 
                 and will need to be rescored based on their likelihood to respond to direct mail 
                 compared to the other At Risk market sector addresses. 
    
                 In addition, when scoring each market sector, 
                 take into account a minimum of 10 addresses per carrier route 
                 and 75 addresses per zip code to stay within postal regulations.

c.	Data Resource to Consider: 
    Need to consider the transactional data and the demographic data for scoring purposes. 

    (i.e.: may want to weed out older customers from mailings who only appear to order when grandchildren are over, 
    identify customers who have a dependent move out in combination with decreasing order frequency.  

    Also take into account the propensity to respond to direct mail 
    and seasonality of ordering based on in-home date of the mail file)

d.	Desired Solution: 
    To have a model that ranks each address in a market sector against the other addresses in a market sector at the time when the mail       file is pulled to select the best customers to mail from the selected market sectors.

e.	Method of Delivery: 
    The ability to run the scoring mechanism at the time of each mail file being pulled based on type of mailing.

