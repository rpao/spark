// 10 miles radius

var result = db.lineorder.aggregate([
   {
      $match: {
          'c_address_geo': {
             $geoWithin: {
                   $centerSphere: [ [ -87.42, 41.24 ], 10/3963.2 ] 
             }
          }
      }
   },
   { $project : { "c_address_pk": 1 } },
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "c_address_pk",
           foreignField: "c_address_fk",
           as: "customer_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$customer_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
   { $project : { "customer_join.c_custkey": 1 } },
   

   {	
       $lookup:
       {
           from: "lineorder",
           localField: "customer_join.c_custkey",
           foreignField: "lo_custkey",
           as: "lineorder_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$lineorder_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
   { $project : { "lineorder_join": 1 } },	
  
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "lineorder_join.lo_orderdate",
           foreignField: "d_datekey",
           as: "final_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$final_join", 
            preserveNullAndEmptyArrays: false
        }
    },   
  
   {
       $group:
       {
            _id : "$final_join.d_year" ,
           count: { $sum: 1 }
       }
   },
   { 
        $sort:
        { 
            _id : 1
        }  
   }
  
]).toArray()

print("results: " + result.length)
printjson(result)