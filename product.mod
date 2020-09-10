// --------------------------------------------------------------------------
// Licensed Materials - Property of IBM
//
// 5725-A06 5725-A29 5724-Y48 5724-Y49 5724-Y54 5724-Y55
// Copyright IBM Corporation 1998, 2013. All Rights Reserved.
//
// Note to U.S. Government Users Restricted Rights:
// Use, duplication or disclosure restricted by GSA ADP Schedule
// Contract with IBM Corp.
// --------------------------------------------------------------------------

{string} Products = ...;
{string} Resources = ...;
tuple productData {
   float demand;
   float insideCost;
   float outsideCost;
   float consumption[Resources];
}
productData Product[Products] = ...;
float Capacity[Resources] = ...;

dvar float+ Inside[Products];
dvar float+ Outside[Products];

execute CPX_PARAM {
  cplex.preind = 0;   
  cplex.simdisplay = 2;   
}


minimize
  sum( p in Products ) 
    (Product[p].insideCost * Inside[p] + 
    Product[p].outsideCost * Outside[p] );
subject to {
  forall( r in Resources )
    ctInside: 
      sum( p in Products ) 
        Product[p].consumption[r] * Inside[p] <= Capacity[r];
  forall( p in Products )
    ctDemand: 
      Inside[p] + Outside[p] >= Product[p].demand;
}

tuple solution {
    string product;
    float inside;
    float outside;
};
{solution} Solution = {<p, Inside[p], Outside[p]> | p in Products};
