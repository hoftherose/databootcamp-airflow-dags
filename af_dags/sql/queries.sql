SELECT COUNT(*) total FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
WHERE location.name in ("California", "New York", "Texas") LIMIT 1000

SELECT os.name, COUNT(*) FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
    JOIN `databootcamp-test1.databootcamp_test1_bq.os` os
    ON review.os_id=os.os_id
WHERE location.name in ("California", "New York", "Texas")
AND os.name in ("Apple iOS", "Apple MacOS")
GROUP BY os.name
LIMIT 1000

SELECT location.name, COUNT(*) total FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
    JOIN `databootcamp-test1.databootcamp_test1_bq.browser` browser
    ON review.browser_id=browser.browser_id
WHERE browser.name = "Chrome"
GROUP BY location.name
ORDER BY total DESC
LIMIT 1000

SELECT location.name, COUNT(*) total FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
    JOIN `databootcamp-test1.databootcamp_test1_bq.date` date
    ON review.date_id=date.date_id
WHERE EXTRACT(YEAR from date.name)=2021
GROUP BY location.name
ORDER BY total DESC
LIMIT 1000

SELECT device.name, COUNT(*) total FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
    JOIN `databootcamp-test1.databootcamp_test1_bq.devices` device
    ON review.device_id=device.device_id
WHERE location.name in ("Maine", "New Hampshire", "Massachusetts", "Rhode Island", "Connecticut", "New York", "New Jersey", "Delaware", "Maryland", "Virginia", "North Carolina", "South Carolina", "Georgia", "Florida")
GROUP BY device.name
ORDER BY total DESC
LIMIT 1000

SELECT device.name, COUNT(*) total FROM
    `databootcamp-test1.databootcamp_test1_bq.review` review
    JOIN `databootcamp-test1.databootcamp_test1_bq.location` location
    ON review.location_id=location.location_id
    JOIN `databootcamp-test1.databootcamp_test1_bq.devices` device
    ON review.device_id=device.device_id
WHERE location.name in ("Alaska", "Arizona", "California", "Colorado", "Hawaii", "Idaho", "Montana", "Nevada", "New Mexico", "Oregon", "Utah", "Washington", "Wyoming")
GROUP BY device.name
ORDER BY total DESC
LIMIT 1000