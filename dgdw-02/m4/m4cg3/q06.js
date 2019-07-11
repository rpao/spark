var r = db.region.findOne({region_pk:4},{region_geo:1});

var result = db.region.aggregate([
   {
      $match: {
          region_geo: {
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
           from: "nation",
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
           from: "city",
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
           from: "customer",
           localField: "city_join.city_pk",
           foreignField: "c_city_fk",
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
           from: "part",
           localField: "lineorder_join.lo_partkey",
           foreignField: "p_partkey",
           as: "part_join"
       }
   },   
   {
        $unwind: 
        { 
            path: "$part_join", 
            preserveNullAndEmptyArrays: false
        }  
   },     
   {
       $match: 
       {
            'part_join.p_category': "MFGR#25"
       }
   },  
   {	
       $lookup:
       {
           from: "date",
           localField: "lineorder_join.lo_orderdate",
           foreignField: "d_datekey",
           as: "date_join"
       }
   },   
   {
        $unwind: 
        { 
            path: "$date_join", 
            preserveNullAndEmptyArrays: false
        }  
   },
   {
       $group:
       {
            _id : { year: "$date_join.d_year" ,  p_brand1: "$part_join.p_brand1"},
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

