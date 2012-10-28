#!/usr/bin/python

#Enter database info here
host = "localhost"
user = "root"
password = "coolio"
db = "datamining"
suffix = "2008"

# Limit number of contributors per candidate ... the queries can get huge & slow
limit = ""#"LIMIT 25"#"LIMIT 1000"

#tha code
import MySQLdb as mdb
import sys
import argparse
import string

allcans = False

def make_arff_header_indiv( cid=0):
  #Make Nominal
  cur.execute("SELECT candidate_id FROM cand"+suffix+" GROUP BY candidate_id;")
  rows = cur.fetchall()
  f = 1
  for row in rows:
    if f:
     s = row["candidate_id"]
     f = 0
    else:
     s = s+","+row["candidate_id"]
  
  nomstr = "{%s}" % s
  #WL?
  wl = ''
  if args.winlose:
    wl = "@ATTRIBUTE class {W,L}\x0A"
  
  #MULTI INSTANCE ARFF
  if args.multi:
    ofile.write("\
@relation CAND_ALL_INDIV\x0A\
@attribute candidate_id "+nomstr+"\x0A\
@attribute bag relational\x0A\
  @ATTRIBUTE amount NUMERIC\x0A\
  @ATTRIBUTE transactiontype {T10, T11, T12, T13, T15, T15C, T15E, T15F, T15I, T15J, T15T, T15Z, T16C, T16F, T16G, T16H, T16J, T16K, T16L, T16R, T17R, T16U, T17Y, T17Z, T18G, T18H, T18J, T18K, T18S, T18U, T19, T19J, T20, T20A, T20B, T20C, T20D, T20F, T20G, T20R, T20V, T22G, T22H, T22J, T22K, T22L, T22R, T22U, T22X, T22Y, T22Z, T23Y, T24A, T24C, T24F, T24G, T24E, T24H, T24I, T24K, T24N, T24P, T24R, T24T, T24U, T24Z, T29}\x0A\
  @ATTRIBUTE employer STRING\x0A\
  @ATTRIBUTE occupation STRING\x0A\
  @ATTRIBUTE entity_type {IND, ORG, CAN, PAC, CCM, COM, PTY}\x0A\
  @ATTRIBUTE party {AIP,AMP,CIT,CON,CRV,CST,DEM,DFL,FRE,GOP,GRE,IAP,IND,JCN,LBU,LIB,N/A,NLP,NNE,NPA,OTH,PAF,REF,REP,RTL,SOC,SWP,TLP,TX,UNI,UNK}\x0A\
  @ATTRIBUTE incum_challenger_openseat {I, C, O}\x0A\
@end bag\x0A\
"+wl+"\
@data\x0A")
  #propositional ARFF
  else:
    if args.occupation:
      h="@RELATION CAND_ALL_INDIV\x0A\
@ATTRIBUTE candidate_id "+nomstr+"\x0A\
@ATTRIBUTE amount NUMERIC\x0A\
@ATTRIBUTE profession STRING\x0A\
@ATTRIBUTE p_amount NUMERIC\x0A\
@ATTRIBUTE party {AIP,AMP,CIT,CON,CRV,CST,DEM,DFL,FRE,GOP,GRE,IAP,IND,JCN,LBU,LIB,N/A,NLP,NNE,NPA,OTH,PAF,REF,REP,RTL,SOC,SWP,TLP,TX,UNI,UNK}\x0A\
@ATTRIBUTE incum_challenger_openseat {I,C,O,N/A}\x0A\
"+wl+"\
@data\x0A"
    elif args.employer:
      h="@RELATION CAND_ALL_INDIV\x0A\
@ATTRIBUTE candidate_id "+nomstr+"\x0A\
@ATTRIBUTE amount NUMERIC\x0A\
@ATTRIBUTE profession STRING\x0A\
@ATTRIBUTE p_amount NUMERIC\x0A\
@ATTRIBUTE party {AIP,AMP,CIT,CON,CRV,CST,DEM,DFL,FRE,GOP,GRE,IAP,IND,JCN,LBU,LIB,N/A,NLP,NNE,NPA,OTH,PAF,REF,REP,RTL,SOC,SWP,TLP,TX,UNI,UNK}\x0A\
@ATTRIBUTE incum_challenger_openseat {I,C,O,N/A}\x0A\
"+wl+"\
@data\x0A"
    else:
      h="@RELATION CAND_ALL_INDIV\x0A\
@ATTRIBUTE candidate_id "+nomstr+"\x0A\
@ATTRIBUTE amount NUMERIC\x0A\
@ATTRIBUTE transactiontype {TN/A,T10,T11,T12,T13,T15,T15C,T15E,T15F,T15I,T15J,T15T,T15Z,T16C,T16F,T16G,T16H,T16J,T16K,T16L,T16R,T17R,T16U,T17Y,T17Z,T18G,T18H,T18J,T18K,T18S,T18U,T19,T19J,T20,T20A,T20B,T20C,T20D,T20F,T20G,T20R,T20V,T22G,T22H,T22J,T22K,T22L,T22R,T22U,T22X,T22Y,T22Z,T23Y,T24A,T24C,T24F,T24G,T24E,T24H,T24I,T24K,T24N,T24P,T24R,T24T,T24U,T24Z,T29}\x0A\
@ATTRIBUTE employer STRING\x0A\
@ATTRIBUTE occupation STRING\x0A\
@ATTRIBUTE entity_type {N/A,IND,ORG,CAN,PAC,CCM,COM,PTY}\x0A\
@ATTRIBUTE party {AIP,AMP,CIT,CON,CRV,CST,DEM,DFL,FRE,GOP,GRE,IAP,IND,JCN,LBU,LIB,N/A,NLP,NNE,NPA,OTH,PAF,REF,REP,RTL,SOC,SWP,TLP,TX,UNI,UNK}\x0A\
@ATTRIBUTE incum_challenger_openseat {I,C,O,N/A}\x0A\
"+wl+"\
@data\x0A"
    ofile.write(h)

def make_arff_body_instance( cid, amount, trans_type, entity_type, party, ico, wl=0, profession=0, p_amount=0):
  if args.multi:
    l = "%s, T%s, '%s', '%s', %s, %s, %s" %( amount, trans_type, employer, occupation, entity_type, party, ico )
  else:
    if len(trans_type) == 0:
      trans_type = "N/A"
    if entity_type == '':
      entity_type = 'N/A'
    if party == '':
      party = 'N/A'
    if amount == '':
      amount = 0
    if  ico == '' or ico.isdigit():
      ico = "N/A"
    else:
      ico = ico.upper()
    if wl == 0 or wl == '':
      wl = ""
    else:
      wl = wl.upper()
  if args.occupation:
   l = "%s,%s,\"%s\",%s,%s,%s%s\x0A" %( cid, amount, profession.replace(',', '').replace('"','').replace("'","").replace('\\', '/'), p_amount, party.upper(), ico.upper(), wl )
  elif args.employer:
   l = "%s,%s,\"%s\",%s,%s,%s%s\x0A" %( cid, amount, profession.replace(',', '').replace('"','').replace("'","").replace('\\', '/'), p_amount, party.upper(), ico.upper(), wl )
  ofile.write( l)
   
def make_arff_body_instance_all( cid, amount, trans_type, entity_type, party, ico, wl=0, employer=0, occupation=0):
  if args.multi:
    l = "%s, T%s, '%s', '%s', %s, %s, %s" %( amount, trans_type, employer, occupation, entity_type, party, ico )
  else:
    if len(trans_type) == 0:
      trans_type = "N/A"
    if entity_type == '':
      entity_type = 'N/A'
    if party == '':
      party = 'N/A'
    if amount == '':
      amount = 0
    if  ico == '' or ico.isdigit():
      ico = "N/A"
    else:
      ico = ico.upper()
    if wl == 0 or wl == '':
      wl = ""
    else:
      wl = wl.upper()
  l = "%s,%s,T%s,\"%s\",\"%s\",\"%s\",%s,%s,%s\x0A" %( cid, amount, trans_type, employer.replace(',', '').replace('"','').replace("'","").replace('\\', '/'), occupation.replace(',', '').replace('"','').replace("'","").replace('\\', '/'), entity_type.upper(), party.upper(), ico.upper(), wl.upper() )
  ofile.write( l)

parser = argparse.ArgumentParser()
parser = argparse.ArgumentParser(description="This program will trace backwards through the FEC campaign contributor data and find out where the money came from. It can group data by employer or occupation, for use in data mining. It assumes data is in a SQL database (edit file for config). by Brandon Roberts, Editor, The Austin Cut. copyleft 2012")
group = parser.add_mutually_exclusive_group()
group.add_argument("-o", "--occupation", action="store_true", help="group contributions by occupation")
group.add_argument("-e", "--employer", action="store_true", help="group contributions by employer")
parser.add_argument("--against", action="store_true", help="toggle to only display contributions against (default is pro)", default=0)
parser.add_argument("-c", "--csv", action="store_true", help="write output to csv file", default=0)
parser.add_argument("-f", "--arff", action="store_true", help="write output to arff file", default=0)
parser.add_argument("-m", "--multi", action="store_true", help="output multi-instance format, strings will be ignored", default=0)
parser.add_argument("-w", "--winlose", action="store_true", help="attach win/lose information to output", default=0)
parser.add_argument("-v", "--verbose", action="store_true", help="output more stuff, not good is you're piping to disk", default=0)
parser.add_argument("name", type=str, help="the name of the politician you want to look up in the format used in the DB use 'all' for all that can be found ... FEC case: LAST, FIRST")
parser.add_argument("out_file", type=str, help="file name to write out new file")
args = parser.parse_args()

# Set up candidate
candidate = args.name
if candidate == "all":
  allcans = True  

if args.verbose: print "[*] Using Candidate : %s" % candidate

con = None

try:
  # Connect
  con = mdb.connect(host, user, password, db)
  cur = con.cursor(mdb.cursors.DictCursor)
  
  # Open Out File
  if args.out_file:
    ofile = open(args.out_file, "w")
  
  # Make query based on all or just one candidate
  if allcans:
    if args.winlose:
      q = "SELECT cand"+suffix+".candidate_id FROM cand"+suffix+" JOIN cansum"+suffix+" ON cand"+suffix+".candidate_id=cansum"+suffix+".candidate_id WHERE cansum"+suffix+".general_elec_status != '';"
    else:
      q = "SELECT candidate_id FROM cand"+suffix+";"
  else:
    q = "SELECT candidate_id FROM cand"+suffix+" WHERE candidate_name = \"%s\";" % candidate
  if args.verbose: print "[*]\t- SQL Query : %s" % q
  # Grab the IDs
  cur.execute(q)
  rs = cur.fetchall()

  # Sometimes people aren't in the cand, but are in the cansum
  try:
    cid = rs[0]["candidate_id"]
  except:
    q = "SELECT candidate_id FROM cansum"+suffix+" WHERE candidate_name = \"%s\";" % candidate
    cur.execute(q)
    rs = cur.fetchall()

  #Write Header of File
  if args.arff:
    # header
    if allcans:
      make_arff_header_indiv()
    else:
      make_arff_header_indiv(cid)
  else:
    print '"name",amount,"transaction_date","transaction_type","employer","occupation","entity_type","party","incum_challenger_openseat","general_elec_status"'

  # Run Through Fetched Candidates
  for r in rs:
    cid = r["candidate_id"]
    if args.verbose: print "[*] Candidate ID : %s " % cid

    # Find all affiliated committees
    q = "SELECT committee_id FROM ccl"+suffix+" WHERE candidate_id = \"%s\";" % cid
    cur.execute(q)
    rows = cur.fetchall()
    filers = []

    # Sort Through Affiliated Committees
    for row in rows:
      committee_id = row["committee_id"]
      if args.verbose: print "[*] Committee ID : %s" % committee_id
      
      # Grab Filers Associated to Committee
      q1 = "SELECT filer_id FROM pas"+suffix+" WHERE other_id=\"%s\";" % committee_id
      cur.execute(q1)
      rows1 = cur.fetchall()
      if args.verbose: print "[*] SQL Query: %s"%q1
      if args.verbose: print "[*] There are %d filers (for this committee)." % len(rows1)

      # Make sure there are filers attached to this committee
      no = False
      try:
        if len(rows1) == 0:
          no = True
      except NameError:
        if args.verbose: print "[!] No contributions (what a loser!) ... skipping"
        break

      if no:
        if args.verbose: print "[!] NO ROWS! We're gonna set values directly"
        filer_id = committee_id
        filer_string = "filer_id = '%s'" % filer_id
        if args.verbose: print "[*]\t- Filer_ID: %s"% filer_id

      # Gather All Filers to Query
      went = False
      f = 0
      for row1 in rows1: 
        went = True
        filer_id = row1["filer_id"]
        if f == 0:
          filer_string = "filer_id = '%s'" % filer_id
          f = 1
        else:
          if string.find(filer_string, filer_id) == -1:
            filer_string = "%s OR filer_id = '%s'" %( filer_string, filer_id )
            
      if args.verbose: print "[*]\t- Filer String: %s" % filer_string
      
      if args.against:
        against = "AND transaction_type='24A' AND transaction_type='24N' AND transaction_type='22Y'"
      else:
        against = "AND transaction_type!='24A' AND transaction_type!='24N' AND transaction_type!='22Y'"

      if args.employer:
        q2 = "SELECT *, count(employer), SUM(amount) FROM indiv"+suffix+" WHERE %s %s GROUP BY employer ORDER BY count(employer) DESC %s;" % (filer_string, against, limit)
      elif args.occupation:
        q2 = "SELECT *, count(occupation), SUM(amount) FROM indiv"+suffix+" WHERE %s %s GROUP BY occupation ORDER BY count(occupation) DESC %s;" % (filer_string, against, limit)
      else:
        q2 = "SELECT * FROM indiv"+suffix+" WHERE %s %s %s;" % (filer_string, against, limit)
      if args.verbose: print "[*] Our SQL Query: %s" % q2

      cur.execute(q2)
      rows2 = cur.fetchall()
      if args.verbose: print "[*]\t- Rows: %s" % len(rows2)
      
      # for text formatting
      n = 0
      a = 0
      i = 50
      i2 = 50
      i3 = 20
      header = 0
      wl = ''

      #Output start of multi instance ARFF
      if args.multi: ofile.write( "%s,\"" % cid)
      
      # Display our instances themselves
      
      # ... in ARFF or CSV format
      if args.arff or args.csv:
        for row2 in rows2:
          #Get candidate data
          qc = "SELECT * FROM cand"+suffix+" WHERE candidate_id='%s';" %cid
          cur.execute(qc)
          qcrow = cur.fetchone()
          
          if args.winlose:
            qs = "SELECT * FROM cansum"+suffix+" WHERE candidate_id='%s';" %cid
            cur.execute(qs)
            qsrow = cur.fetchone()

          if wl == '' and args.winlose:
            wl = qsrow["general_elec_status"]
          
          #Employer
          if args.employer:
            make_arff_body_instance( cid, row2["SUM(amount)"], row2["transaction_type"], row2["entity_type"], qcrow["party"], qcrow["incum_challenger_openseat"], wl, row2["employer"], row2["count(employer)"] )
          #Occupation
          elif args.occupation:
            make_arff_body_instance( cid, row2["SUM(amount)"], row2["transaction_type"], row2["entity_type"], qcrow["party"], qcrow["incum_challenger_openseat"], wl, row2["occupation"], row2["count(occupation)"] )
          # All Indiv
          else:
            make_arff_body_instance_all( cid, row2["amount"], row2["transaction_type"], row2["entity_type"], qcrow["party"], qcrow["incum_challenger_openseat"], wl, row2["employer"], row2["occupation"] )

          if args.multi:
            if( len(rows2) != a+1 ):
              if args.multi:
                ofile.write( "\\n")
              else:
                ofile.write( "\n")
          a = a + 1
      # .. or in text mode
      else:
        for row2 in rows2:
          a  = ""
          a2 = ""
          a3 = ""
          if len(row2["name"]) > i:
            i = len(row2["name"]) + 1
          while len(row2["name"]+a) < i:
            a = a + " "
          if len(row2["employer"]) > i2:
            i2 = len(row2["employer"]) + 1
          while len(row2["employer"]+a2) < i2:
            a2 = a2 + " "
          if len(row2["occupation"]) > i3:
            i3 = len(row2["occupation"]) + 1
          while len(row2["occupation"]+a3) < i3:
            a3 = a3 + " "
          if args.employer:
            print "%d\t%s%s\t%s\t$%s" %(n, row2["employer"],a2, row2["count(employer)"], row2["SUM(amount)"] )
          elif args.occupation:
           print "%d\t%s%s\t%s\t$%s" %(n, row2["occupation"],a3, row2["count(occupation)"], row2["SUM(amount)"] )
          else:
            print "%d\t%s%s\t$%s\t%s\t%s\t%s%s  %s  %s%s" %(n, row2["name"],a, row2["amount"], row2["transaction_date"], row2["transaction_type"], row2["employer"],a2, row2["occupation"],a3, row2["transaction_id"])
          n = n + 1

  #End Instance File
  if args.multi: ofile.write("\"," +wl)

except mdb.Error, e:
  print "Error %d: %s" % (e.args[0],e.args[1])
  sys.exit(1)

if args.verbose: print "[*] Done."
if con:    
  con.close()
