
var c = db.lineorder.findOne({city_pk:20});
var result = db.lineorder.aggregate([
   {
      $match: {
          c_address_geo: {
             $geoWithin: {
                $geometry: c.city_geo
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
            'lineorder_join.p_category': "MFGR#25"
       }
   },    
   {
       $group:
       {
            _id : { year: "$lineorder_join.d_year" ,  p_brand1: "$lineorder_join.p_brand1"},
           total_revenue: { $sum: "$lineorder_join.lo_revenue" }
       }
   },
   { 
        $sort:
        { 
           '_id.year' : 1,
            '_id.p_brand1': 1
        }
   }
]).toArray()

print("results = " + result.length)
printjson(result)



