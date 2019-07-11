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
   { $match : { 'c_address_pk': {$exists:true } }},     
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "c_address_pk",
           foreignField: "c_address_fk",
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
       $match: 
       {
            'lineorder_join.p_category': "MFGR#24"
       }
   }, 	
   {
       $group:
       {
            _id : "$lineorder_join.d_year" ,
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



