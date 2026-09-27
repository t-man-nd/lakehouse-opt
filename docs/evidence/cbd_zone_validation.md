# CBD zone list vs. fees charged in the data

Trips starting inside the Congestion Relief Zone after 2025-01-05 always owe the CBD fee, so the share of fee-paying pickups per zone tests the zone list independently of its source.

Verdict counts: CONFIRMED_BY_FEES 38, TOO_FEW_TRIPS 36, CONSISTENT_NON_CBD 187

| Zone | Name | Listed as CBD | List source | Post-policy pickups | Fee share | Verdict |
|---:|---|---|---|---:|---:|---|
| 211 | SoHo | True | mta_yfdc_w5jh_partial | 89,268 | 0.9965 | CONFIRMED_BY_FEES |
| 114 | Greenwich Village South | True | mta_yfdc_w5jh_partial | 164,022 | 0.9961 | CONFIRMED_BY_FEES |
| 234 | Union Sq | True | inferred_geography | 291,409 | 0.9961 | CONFIRMED_BY_FEES |
| 249 | West Village | True | inferred_geography | 243,450 | 0.996 | CONFIRMED_BY_FEES |
| 161 | Midtown Center | True | mta_yfdc_w5jh_partial | 479,186 | 0.9959 | CONFIRMED_BY_FEES |
| 113 | Greenwich Village North | True | mta_yfdc_w5jh_partial | 151,481 | 0.9959 | CONFIRMED_BY_FEES |
| 79 | East Village | True | mta_yfdc_w5jh_partial | 263,897 | 0.9956 | CONFIRMED_BY_FEES |
| 144 | Little Italy/NoLiTa | True | mta_yfdc_w5jh_partial | 115,270 | 0.9955 | CONFIRMED_BY_FEES |
| 163 | Midtown North | True | mta_yfdc_w5jh_partial | 276,561 | 0.9954 | CONFIRMED_BY_FEES |
| 164 | Midtown South | True | mta_yfdc_w5jh_partial | 225,103 | 0.9953 | CONFIRMED_BY_FEES |
| 162 | Midtown East | True | mta_yfdc_w5jh_partial | 338,944 | 0.9951 | CONFIRMED_BY_FEES |
| 148 | Lower East Side | True | mta_yfdc_w5jh_partial | 121,722 | 0.9945 | CONFIRMED_BY_FEES |
| 229 | Sutton Place/Turtle Bay North | True | mta_yfdc_w5jh_partial | 167,645 | 0.9942 | CONFIRMED_BY_FEES |
| 230 | Times Sq/Theatre District | True | mta_yfdc_w5jh_partial | 344,229 | 0.9941 | CONFIRMED_BY_FEES |
| 48 | Clinton East | True | mta_yfdc_w5jh_partial | 248,611 | 0.9941 | CONFIRMED_BY_FEES |
| 68 | East Chelsea | True | mta_yfdc_w5jh_partial | 272,588 | 0.9939 | CONFIRMED_BY_FEES |
| 186 | Penn Station/Madison Sq West | True | mta_yfdc_w5jh_partial | 339,854 | 0.9938 | CONFIRMED_BY_FEES |
| 90 | Flatiron | True | mta_yfdc_w5jh_partial | 163,920 | 0.9934 | CONFIRMED_BY_FEES |
| 100 | Garment District | True | mta_yfdc_w5jh_partial | 147,741 | 0.9924 | CONFIRMED_BY_FEES |
| 125 | Hudson Sq | True | mta_yfdc_w5jh_partial | 53,807 | 0.9923 | CONFIRMED_BY_FEES |
| 170 | Murray Hill | True | mta_yfdc_w5jh_partial | 281,620 | 0.9921 | CONFIRMED_BY_FEES |
| 107 | Gramercy | True | mta_yfdc_w5jh_partial | 206,303 | 0.9921 | CONFIRMED_BY_FEES |
| 158 | Meatpacking/West Village West | True | mta_yfdc_w5jh_partial | 103,454 | 0.9915 | CONFIRMED_BY_FEES |
| 233 | UN/Turtle Bay South | True | inferred_geography | 112,862 | 0.9904 | CONFIRMED_BY_FEES |
| 224 | Stuy Town/Peter Cooper Village | True | mta_yfdc_w5jh_partial | 19,248 | 0.9885 | CONFIRMED_BY_FEES |
| 87 | Financial District North | True | mta_yfdc_w5jh_partial | 58,790 | 0.9845 | CONFIRMED_BY_FEES |
| 231 | TriBeCa/Civic Center | True | mta_yfdc_w5jh_partial | 142,222 | 0.9842 | CONFIRMED_BY_FEES |
| 246 | West Chelsea/Hudson Yards | True | inferred_geography | 188,886 | 0.9836 | CONFIRMED_BY_FEES |
| 12 | Battery Park | True | mta_yfdc_w5jh_partial | 2,561 | 0.9832 | CONFIRMED_BY_FEES |
| 137 | Kips Bay | True | mta_yfdc_w5jh_partial | 115,334 | 0.9794 | CONFIRMED_BY_FEES |
| 4 | Alphabet City | True | mta_yfdc_w5jh_partial | 27,496 | 0.9791 | CONFIRMED_BY_FEES |
| 50 | Clinton West | True | mta_yfdc_w5jh_partial | 59,419 | 0.979 | CONFIRMED_BY_FEES |
| 13 | Battery Park City | True | mta_yfdc_w5jh_partial | 62,535 | 0.9745 | CONFIRMED_BY_FEES |
| 261 | World Trade Center | True | inferred_geography | 47,533 | 0.972 | CONFIRMED_BY_FEES |
| 209 | Seaport | True | mta_yfdc_w5jh_partial | 19,245 | 0.9689 | CONFIRMED_BY_FEES |
| 45 | Chinatown | True | mta_yfdc_w5jh_partial | 18,983 | 0.9685 | CONFIRMED_BY_FEES |
| 88 | Financial District South | True | mta_yfdc_w5jh_partial | 29,789 | 0.9615 | CONFIRMED_BY_FEES |
| 232 | Two Bridges/Seward Park | True | mta_yfdc_w5jh_partial | 20,676 | 0.9483 | CONFIRMED_BY_FEES |
| 199 | Rikers Island | False | not_cbd | 5 | 1.0 | TOO_FEW_TRIPS |
| 251 | Westerleigh | False | not_cbd | 28 | 0.3929 | TOO_FEW_TRIPS |
| 187 | Port Richmond | False | not_cbd | 9 | 0.3333 | TOO_FEW_TRIPS |
| 245 | West Brighton | False | not_cbd | 16 | 0.3125 | TOO_FEW_TRIPS |
| 84 | Eltingville/Annadale/Prince's Bay | False | not_cbd | 7 | 0.2857 | TOO_FEW_TRIPS |
| 128 | Inwood Hill Park | False | not_cbd | 143 | 0.2657 | TOO_FEW_TRIPS |
| 206 | Saint George/New Brighton | False | not_cbd | 32 | 0.25 | TOO_FEW_TRIPS |
| 8 | Astoria Park | False | not_cbd | 62 | 0.2097 | TOO_FEW_TRIPS |
| 204 | Rossville/Woodrow | False | not_cbd | 5 | 0.2 | TOO_FEW_TRIPS |
| 221 | Stapleton | False | not_cbd | 119 | 0.1933 | TOO_FEW_TRIPS |
| 176 | Oakwood | False | not_cbd | 11 | 0.1818 | TOO_FEW_TRIPS |
| 2 | Jamaica Bay | False | not_cbd | 11 | 0.1818 | TOO_FEW_TRIPS |
| 120 | Highbridge Park | False | not_cbd | 122 | 0.1639 | TOO_FEW_TRIPS |
| 115 | Grymes Hill/Clifton | False | not_cbd | 63 | 0.1587 | TOO_FEW_TRIPS |
| 172 | New Dorp/Midland Beach | False | not_cbd | 51 | 0.1569 | TOO_FEW_TRIPS |
| 111 | Green-Wood Cemetery | False | not_cbd | 20 | 0.15 | TOO_FEW_TRIPS |
| 118 | Heartland Village/Todt Hill | False | not_cbd | 48 | 0.1458 | TOO_FEW_TRIPS |
| 156 | Mariners Harbor | False | not_cbd | 22 | 0.1364 | TOO_FEW_TRIPS |
| 31 | Bronx Park | False | not_cbd | 159 | 0.1195 | TOO_FEW_TRIPS |
| 109 | Great Kills | False | not_cbd | 9 | 0.1111 | TOO_FEW_TRIPS |
| 58 | Country Club | False | not_cbd | 94 | 0.0957 | TOO_FEW_TRIPS |
| 214 | South Beach/Dongan Hills | False | not_cbd | 88 | 0.0909 | TOO_FEW_TRIPS |
| 23 | Bloomfield/Emerson Hill | False | not_cbd | 61 | 0.082 | TOO_FEW_TRIPS |
| 59 | Crotona Park | False | not_cbd | 49 | 0.0816 | TOO_FEW_TRIPS |
| 253 | Willets Point | False | not_cbd | 63 | 0.0794 | TOO_FEW_TRIPS |
| 240 | Van Cortlandt Park | False | not_cbd | 194 | 0.0722 | TOO_FEW_TRIPS |
| 96 | Forest Park/Highland Park | False | not_cbd | 138 | 0.0652 | TOO_FEW_TRIPS |
| 46 | City Island | False | not_cbd | 114 | 0.0614 | TOO_FEW_TRIPS |
| 57 | Corona | False | not_cbd | 157 | 0.0573 | TOO_FEW_TRIPS |
| 184 | Pelham Bay Park | False | not_cbd | 55 | 0.0545 | TOO_FEW_TRIPS |
| 154 | Marine Park/Floyd Bennett Field | False | not_cbd | 146 | 0.0479 | TOO_FEW_TRIPS |
| 27 | Breezy Point/Fort Tilden/Riis Beach | False | not_cbd | 28 | 0.0357 | TOO_FEW_TRIPS |
| 30 | Broad Channel | False | not_cbd | 49 | 0.0204 | TOO_FEW_TRIPS |
| 5 | Arden Heights | False | not_cbd | 2 | 0.0 | TOO_FEW_TRIPS |
| 44 | Charleston/Tottenville | False | not_cbd | 6 | 0.0 | TOO_FEW_TRIPS |
| 99 | Freshkills Park | False | not_cbd | 1 | 0.0 | TOO_FEW_TRIPS |
| 264 | N/A | False | not_cbd | 22,025 | 0.617 | CONSISTENT_NON_CBD |
| 43 | Central Park | False | not_cbd | 133,079 | 0.5929 | CONSISTENT_NON_CBD |
| 142 | Lincoln Square East | False | not_cbd | 308,221 | 0.5895 | CONSISTENT_NON_CBD |
| 237 | Upper East Side South | False | not_cbd | 464,825 | 0.5197 | CONSISTENT_NON_CBD |
| 138 | LaGuardia Airport | False | not_cbd | 265,111 | 0.5136 | CONSISTENT_NON_CBD |
| 143 | Lincoln Square West | False | not_cbd | 114,707 | 0.5135 | CONSISTENT_NON_CBD |
| 70 | East Elmhurst | False | not_cbd | 28,668 | 0.5129 | CONSISTENT_NON_CBD |
| 66 | DUMBO/Vinegar Hill | False | not_cbd | 7,808 | 0.5019 | CONSISTENT_NON_CBD |

Non-CBD zones with a fee share below 0.5 are omitted from the table.
