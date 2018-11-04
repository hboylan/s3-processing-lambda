// flatten yelp_academic_dataset_business.json

const util = require('../util')

const getValidJson = str => JSON.parse(
    str
        .replace(/'/g, '"')
        .replace(/True/g, 'true')
        .replace(/False/g, 'false')
)

module.exports = function(line, lineNumber) {
    const json = JSON.parse(line)
    const attr = json.attributes
    const hours = json.hours
    
    // defaults
    json.is_romantic = null
    json.is_intimate = null
    json.is_classy = null
    json.is_hipster = null
    json.is_divey = null
    json.is_touristy = null
    json.is_trendy = null
    json.is_upscale = null
    json.is_casual = null
    json.is_cc_friendly = null
    json.is_kid_friendly = null
    json.is_dog_friendly = null
    json.is_group_friendly = null
    json.is_wheelchair_accessible = null

    json.has_garage = null
    json.has_street_parking = null
    json.has_validated_parking = null
    json.has_parking_lot = null
    json.has_bike_parking = null
    json.has_valet = null
    json.has_catering = null
    json.has_dessert = null
    json.has_late_night = null
    json.has_lunch = null
    json.has_dinner = null
    json.has_breakfast = null
    json.has_brunch = null
    json.has_tv = null
    json.has_outdoor_seating = null
    json.has_reservations = null
    json.has_table_service = null
    json.has_wifi = null
    json.has_alcohol = null
    json.has_delivery = null
    json.has_takeout = null

    json.alcohol_type = null
    json.noise_level = null
    json.attire = null
    json.price_range = null

    json.hours_sunday = json.hours_monday = json.hours_tuesday = json.hours_wednesday = json.hours_thursday = json.hours_friday = json.hours_saturday = null

    // attributes
    if (attr) {

        json.is_cc_friendly = attr.BusinessAcceptsCreditCards ? attr.BusinessAcceptsCreditCards == 'true' : null
        json.is_kid_friendly = attr.GoodForKids ? attr.GoodForKids == 'true' : null
        json.is_dog_friendly = attr.DogsAllowed ? attr.DogsAllowed == 'true' : null
        json.is_group_friendly = attr.RestaurantsGoodForGroups ? attr.RestaurantsGoodForGroups == 'true' : null
        json.is_wheelchair_accessible = attr.WheelchairAccessible ? attr.WheelchairAccessible == 'true' : null
        json.has_bike_parking = attr.BikeParking ? attr.BikeParking == 'true' : null
        json.has_catering = attr.Caters ? attr.Caters == 'true' : null
        json.has_tv = attr.HasTV ? attr.HasTV == 'true' : null
        json.has_outdoor_seating = attr.OutdoorSeating ? attr.OutdoorSeating == 'true' : null
        json.has_reservations = attr.RestaurantsReservations ? attr.RestaurantsReservations == 'true' : null
        json.has_table_service = attr.RestaurantsTableService ? attr.RestaurantsTableService == 'true' : null
        json.has_wifi = attr.WiFi ? attr.WiFi == 'yes' : null
        json.has_alcohol = attr.Alcohol ? attr.Alcohol != 'none' : null
        json.has_delivery = attr.RestaurantsDelivery ? attr.RestaurantsDelivery == 'true' : null
        json.has_takeout = attr.RestaurantsTakeOut ? attr.RestaurantsTakeOut == 'true' : null
        json.alcohol_type = attr.Alcohol || null
        json.noise_level = attr.NoiseLevel || null
        json.attire = attr.RestaurantAttire || null
        json.price_range = attr.RestaurantsPriceRange2 ? parseInt(attr.RestaurantsPriceRange2) : null


        // ambience
        if (attr.Ambience) {
            attr.Ambience = getValidJson(attr.Ambience)
            json.is_romantic = attr.Ambience.romantic === undefined ? null : attr.Ambience.romantic
            json.is_intimate = attr.Ambience.intimate === undefined ? null : attr.Ambience.intimate
            json.is_classy = attr.Ambience.classy === undefined ? null : attr.Ambience.classy
            json.is_hipster = attr.Ambience.hipster === undefined ? null : attr.Ambience.hipster
            json.is_divey = attr.Ambience.divey === undefined ? null : attr.Ambience.divey
            json.is_touristy = attr.Ambience.touristy === undefined ? null : attr.Ambience.touristy
            json.is_trendy = attr.Ambience.trendy === undefined ? null : attr.Ambience.trendy
            json.is_upscale = attr.Ambience.upscale === undefined ? null : attr.Ambience.upscale
            json.is_casual = attr.Ambience.casual === undefined ? null : attr.Ambience.casual
        }

        // parking
        if (attr.BusinessParking) {
            attr.BusinessParking = getValidJson(attr.BusinessParking)
            json.has_garage = attr.BusinessParking.garage === undefined ? null : attr.BusinessParking.garage
            json.has_street_parking = attr.BusinessParking.street === undefined ? null : attr.BusinessParking.street
            json.has_validated_parking = attr.BusinessParking.validated === undefined ? null : attr.BusinessParking.validated
            json.has_parking_lot = attr.BusinessParking.lot === undefined ? null : attr.BusinessParking.lot
            json.has_valet = attr.BusinessParking.valet === undefined ? null : attr.BusinessParking.valet
        }

        // meals
        if (attr.GoodForMeal) {
            attr.GoodForMeal = getValidJson(attr.GoodForMeal)
            json.has_breakfast = attr.GoodForMeal.breakfast === undefined ? null : attr.GoodForMeal.breakfast
            json.has_lunch = attr.GoodForMeal.lunch === undefined ? null : attr.GoodForMeal.lunch
            json.has_dinner = attr.GoodForMeal.dinner === undefined ? null : attr.GoodForMeal.dinner
            json.has_brunch = attr.GoodForMeal.brunch === undefined ? null : attr.GoodForMeal.brunch
            json.has_late_night = attr.GoodForMeal.latenight === undefined ? null : attr.GoodForMeal.latenight
            json.has_dessert = attr.GoodForMeal.dessert === undefined ? null : attr.GoodForMeal.dessert
        }
    }

    // hours
    if (hours) {
        json.hours_sunday = hours.Sunday || null
        json.hours_monday = hours.Monday || null
        json.hours_tuesday = hours.Tuesday || null
        json.hours_wednesday = hours.Wednesday || null
        json.hours_thursday = hours.Thursday || null
        json.hours_friday = hours.Friday || null
        json.hours_saturday = hours.Saturday || null
    }

    // drop
    delete json.attributes
    delete json.hours
    
    // convert JSON to CSV
    const row = util.csvFromArray(Object.values(json))

    // add CSV header
    if (lineNumber === 1) {
        const header = util.csvFromArray(Object.keys(json))
        return `${header}\n${row}`
    }
    
    return row + '\n'
}