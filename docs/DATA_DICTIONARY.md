# Data Dictionary – NYC Yellow Taxi Trip Records

| **Field Name** | **Description** |
|---------------|-----------------|
| **VendorID** | Code indicating the TPEP provider that supplied the trip record. <br>**1** = Creative Mobile Technologies (CMT) <br>**2** = Curb Mobility <br>**6** = Myle Technologies <br>**7** = Helix |
| **tpep_pickup_datetime** | Date and time when the taximeter was engaged (pickup time). |
| **tpep_dropoff_datetime** | Date and time when the taximeter was disengaged (dropoff time). |
| **passenger_count** | Number of passengers in the vehicle. |
| **trip_distance** | Trip distance reported by the taximeter, measured in **miles**. |
| **RatecodeID** | Final fare rate applied at the end of the trip. <br>**1** = Standard rate <br>**2** = JFK <br>**3** = Newark <br>**4** = Nassau or Westchester <br>**5** = Negotiated fare <br>**6** = Group ride <br>**99** = Null / Unknown |
| **store_and_fwd_flag** | Indicates whether the trip record was stored in the vehicle before being sent to the vendor due to lack of network connection. <br>**Y** = Store and forward <br>**N** = Sent directly |
| **PULocationID** | TLC Taxi Zone where the trip started. |
| **DOLocationID** | TLC Taxi Zone where the trip ended. |
| **payment_type** | Passenger payment type. <br>**0** = Flex Fare trip <br>**1** = Credit card <br>**2** = Cash <br>**3** = No charge <br>**4** = Dispute <br>**5** = Unknown <br>**6** = Voided trip |
| **fare_amount** | Time-and-distance fare calculated by the taximeter. |
| **extra** | Additional surcharges and miscellaneous charges. |
| **mta_tax** | Tax automatically applied based on the metered fare. |
| **tip_amount** | Tip amount recorded for **credit card payments only** (cash tips are not included). |
| **tolls_amount** | Total toll charges paid during the trip. |
| **improvement_surcharge** | Improvement surcharge applied at the start of the trip (introduced in 2015). |
| **total_amount** | Total amount charged to the passenger, excluding cash tips. |
| **congestion_surcharge** | NY State congestion surcharge collected during the trip. |
| **airport_fee** | Airport pickup fee, applied only for pickups at **LaGuardia (LGA)** and **John F. Kennedy (JFK)** airports. |
| **cbd_congestion_fee** | Per-trip charge for the MTA's **Congestion Relief Zone**, effective from **January 5, 2025**. |

---

## Additional Notes

- **Distance unit:** Miles
- **Time columns:** Stored as `datetime`.
- **`RatecodeID = 99`** represents **Null/Unknown** rather than a valid fare type.
- **`payment_type = 0`** represents **Flex Fare**, meaning the fare was determined through an upfront pricing mechanism rather than the standard metered fare.
- **`tip_amount`** only includes electronic (credit card) tips; cash tips are not recorded in the dataset.