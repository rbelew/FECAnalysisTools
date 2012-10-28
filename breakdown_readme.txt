This tool pulls contributor data out of a database
filled with FEC campaign finance data in a variety
of ways (by occupation, employer, win/lose).

The SQL database must be formatted in a specific
way for it to work. Namely, tables must exist in
the same format as the FEC data, e.g.:

cand2008   - candidate table
cansum2008 - candidate summary
ccl2008    - committee / candidate links
indiv2008  - individual contribution data
pas2008    - committee contribution data

The suffix, "2008", can be replaced with w/e
you want. I used year because I also have 2012
data in my DB. The suffix can be set in file.

Here are some examples:

1) Pull Al Franken's contributions, grouped by
   contributor occupation. Don't write to file,
   "none" in last field (sorry, this is a hack,
   unless you use --csv or --arff, it won't write
   to a file no matter what you put there.)

   ./breakdown.py -o "Franken, Al" none

2) Output all of Al Franken's contributors, grouped
   by empoyer, to an ARFF file out.arff

   ./breakdown.py -e -f "Franken, Al" out.arff

3) Print all contributors:

  ./breakdown.py "Franken, Al" none