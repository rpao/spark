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
   {
       $match: 
       {
            'p_category': "MFGR#25"
       }
   },    
   {
       $group:
       {
            _id : { year: "$d_year" ,  p_brand1: "$p_brand1"},
           total_revenue: { $sum: "$lo_revenue" }
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



