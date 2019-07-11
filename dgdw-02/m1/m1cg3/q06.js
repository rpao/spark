var r = db.lineorder.findOne({region_pk:4});

var result = db.lineorder.aggregate([
   {
      $match: {
          'region_geo': {
             $geoIntersects: {
                $geometry: r.region_geo
             }
          }
      }
   },	
   { $project : { "region_pk": 1 } },   
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "region_pk",
           foreignField: "region_fk",
           as: "nation_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$nation_join", 
            preserveNullAndEmptyArrays: false
   	   }
   },
   { $project : { "nation_join.nation_pk": 1 } },     
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "nation_join.nation_pk",
           foreignField: "nation_fk",
           as: "city_join"
       }
   },
   {
        $unwind: 
        { 
            path: "$city_join", 
            preserveNullAndEmptyArrays: false
   	   }
   },
   { $project : { "city_join.city_pk": 1 } },       
   {	
       $lookup:
       {
           from: "lineorder",
           localField: "city_join.city_pk",
           foreignField: "c_city_fk",
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

printjson(result.length)
printjson(result)








