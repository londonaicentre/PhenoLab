# Document with rationale for measurement cutoffs

## Creatinine
Creatinine mostly appears as umol/L, and the conversion from mg/dL is included but there are no reliable data from units of this type.
Cutoffs were set at 7 and 3000
- Lower limit of 7 based on [this paper](https://pmc.ncbi.nlm.nih.gov/articles/PMC8498206/) on neuromuscular disease and creatinine. Their lower quartile is 7. There is a small rise in creatinine below 7 on the histogram until 0, which are physiologically infeasible, suggesting wrong results inputted into this data field
- Upper limit of 3000 based on the histogram, which has very sparse results above 3000, which continue evenly upwards above 12000. The highest levels of creatinine reported are around 3000, with only 5 case reports ever of creatinine above 3300 [reference](https://pmc.ncbi.nlm.nih.gov/articles/PMC7357312/), meaning that values in our data above this level are implausible.
- Note that serum and plasma creatinine are included together, as hospital guidelines suggest they have the same reference ranges [link](https://www.southtees.nhs.uk/services/pathology/tests/creatinine/)

## Blood glucose
- Highest ever recorded blood sugar [here](https://www.guinnessworldrecords.com/news/2023/5/miraculous-survival-of-boy-with-blood-sugar-level-21-times-higher-than-normal-746164) - 147, used as the cutoff
- Online anecdotal reports of lowest ever seen and physiological possibility of low blood sugars due to the shape of the histogram used to inform lower bound of 1.0 

## Blood pressure
Blood pressure upper limit used as study found highest ever [370/360](https://pubmed.ncbi.nlm.nih.gov/7741618/#:~:text=The%20highest%20pressure%20recorded%20in,005).
Lowest used reports and clinical experience of values compatible with life, and a value of 15 for diastolic was used as it is also 2 standard deviations below the mean for a neonate. 

## HbA1c
The maximum HbA1c used was 200, as this is above the maximum lab value of 197 at GSTT. The minimum used was due to the shape of the histogram - it appears that the wrong units are input for some values and this causes a smaller secondary distribution below 15. Therefore 15 was used, and I couldn't find any case reports with values lower than this. 

## Haemoglobin
The maximum haemoglobin set was 260, based on the shape of the histogram and on discussion with a haematologist who advised that the highest values he'd ever seen were 220-230. I coudn't find any case reports in polycythaemia. The lowest value was based on clinical experience and the shape of the histogram again, as some values were input with wrong units so this was the lowest possible value without causing the wrong units to be captured (and a haemoglobin of below 25 is biologically implausible). 