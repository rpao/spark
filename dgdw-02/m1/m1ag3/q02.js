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
   {
       $match: 
       {
            p_category: "MFGR#24"
       }
   }, 	
   {
       $group:
       {
            _id : "$d_year" ,
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

print("results = " + result.length)
printjson(result)



