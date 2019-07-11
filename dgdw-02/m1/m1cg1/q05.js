
var r = db.lineorder.findOne({region_pk:2});
var result = db.lineorder.aggregate([
   {
      $match: {
          'nation_geo': {
             $geoWithin: {
                $geometry: r.region_geo
             }
          }
      }
   },
   { $project : { "nation_pk": 1 } },
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "nation_pk",
           foreignField: "c_nation_fk",
           as: "lineorder_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$lineorder_join", 
            preserveNullAndEmptyArrays: true
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

print(result.length)
printjson(result)








