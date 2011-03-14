from syn_map_import import mysql_syn_import
import MySQLdb
from flatfeature import Bed
import MySQLdb.cursors

def remove_retined_homologs(org1_org1_bed_table, org1_org2_bed_table, cursor): 
    "Creates a new table containg only genes that are not retained between both speices"   
    params = {'org1_org1_bed':org1_org1_bed_table, 'org1_org2_bed': org1_org2_bed_table}  
    grab_unique_genes = "CREATE TABLE IF NOT EXISTS non_retained AS \
                         (SELECT %(org1_org2_bed)s.* \
                         FROM %(org1_org2_bed)s \
                         WHERE %(org1_org2_bed)s.accn \
                         NOT IN (SELECT %(org1_org1_bed)s.accn FROM %(org1_org1_bed)s, %(org1_org2_bed)s \
                         WHERE %(org1_org2_bed)s.accn = %(org1_org1_bed)s.accn))" % params
    cursor.execute(grab_unique_genes)


def insert_gene(sfeat, gene, table_name , cursor):
    insert_genes = "INSERT INTO %s (sfeat, accn, start, end, chr, strand) \
                      VALUES ('%s','%s',%d, %d,'%s','%s')"  %(table_name, sfeat['accn'], gene['accn'], gene['start'], gene['end'], gene['Chr'], gene['Strand'])
    cursor.execute(insert_genes)

def grab_gene_to_right(sfeat, bed_table, cursor):
    sfeat['bed_table'] = bed_table
    # Chr and Strand need to be caps to work 
    right_gene_bed = "SELECT %(bed_table)s.* \
                       FROM %(bed_table)s \
                       WHERE %(bed_table)s.chr = '%(Chr)s' AND \
                       %(bed_table)s.start > %(end)d \
                       ORDER BY %(bed_table)s.start ASC LIMIT 1" %(sfeat)
    print right_gene_bed
    cursor.execute(right_gene_bed)
    right_gene = cursor.fetchone()
    return right_gene

def grab_gene_to_left(sfeat, bed_table, cursor):
    sfeat['bed_table'] = bed_table
    left_gene_bed = "SELECT %(bed_table)s.* \
                       FROM %(bed_table)s \
                       WHERE %(bed_table)s.chr = '%(Chr)s' AND \
                       %(bed_table)s.end < %(start)d \
                       ORDER BY %(bed_table)s.start DESC LIMIT 1" %(sfeat)
    print left_gene_bed
    cursor.execute(left_gene_bed)
    left_gene = cursor.fetchone()
    return left_gene

def merge_tables(cursor):
    merge = "CREATE TABLE merge_genes AS(SELECT DISTINCT left_genes.sfeat, left_genes.accn as left_gene, right_genes.accn as right_gene,\
            left_genes.start as left_start, left_genes.end as left_end, right_genes.start as right_start, \
            right_genes.end as right_end, (right_genes.start - left_genes.end ) as sdiff, right_genes.chr as chr, left_genes.strand as lstrand ,right_genes.strand as rstrand \
            FROM left_genes , right_genes \
            WHERE left_genes.sfeat = right_genes.sfeat AND left_genes.chr = right_genes.chr)"
    cursor.execute(merge)
    
    add_accn_to_table = "ALTER TABLE merge_genes \
                         ADD accn INTEGER AUTO_INCREMENT NOT NULL FIRST, \
                         ADD PRIMARY KEY(accn)"
    cursor.execute(add_accn_to_table)

def find_left_right_gene(syn_map1, bed_table, cursor):
 #   filter_bed_table(syn_map1 , bed_table_name , cursor)
    select_non_retained_genes = "SELECT * FROM non_retained"
    cursor.execute(select_non_retained_genes)
    non_retained = cursor.fetchall()
    for sfeat in non_retained:
        right_gene = grab_gene_to_right(sfeat, bed_table, cursor)
        left_gene = grab_gene_to_left(sfeat, bed_table, cursor)
        if left_gene is None:
            print "left_gene:" ,sfeat['accn']
        else:
            insert_gene(sfeat, left_gene, 'left_genes', cursor)
        if right_gene is None:
            print "right_gene :" , sfeat['accn']
        else:
            insert_gene(sfeat, right_gene , 'right_genes', cursor)
    merge_tables(cursor)

def create_left_right_tables(name ,cursor):
    stmt = "CREATE TABLE IF NOT EXISTS %s (sfeat Varchar(30), accn Varchar(30), \
            start INTEGER, end INTEGER , chr Varchar(30), strand Varchar(30))" % name
    cursor.execute(stmt)



def create_region_table(bed_table_name , syn_map1, cursor):
    """CHANGE here NEXT grab all qfeats independt of strand then filter for if they both change or both stay the same"""
    params = {'bed_table_name': bed_table_name, 'syn_map1' : syn_map1}
    left_region_new = "CREATE TABLE left_region as(SELECT DISTINCT left_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                       FROM (SELECT DISTINCT merge_genes.accn, merge_genes.sfeat , merge_genes.left_gene, merge_genes.lstrand,\
                       %(syn_map1)s.qfeat as left_gene_qfeat FROM merge_genes, %(syn_map1)s WHERE merge_genes.left_gene \
                       = %(syn_map1)s.sfeat) as left_genes , %(bed_table_name)s \
                       WHERE left_genes.left_gene_qfeat = %(bed_table_name)s.accn AND left_genes.lstrand = %(bed_table_name)s.strand)" %params
    print left_region_new                   
    cursor.execute(left_region_new)
    
    right_region_new = "CREATE TABLE right_region as(SELECT DISTINCT right_genes.* , %(bed_table_name)s.start , %(bed_table_name)s.end , %(bed_table_name)s.chr, %(bed_table_name)s.strand \
                      FROM (SELECT DISTINCT merge_genes.accn, merge_genes.sfeat , merge_genes.right_gene, merge_genes.rstrand , %(syn_map1)s.qfeat as right_gene_qfeat\
                      FROM merge_genes, %(syn_map1)s \
                      WHERE merge_genes.right_gene = %(syn_map1)s.sfeat) as right_genes , %(bed_table_name)s \
                      WHERE right_genes.right_gene_qfeat = %(bed_table_name)s.accn AND right_genes.rstrand = %(bed_table_name)s.strand)" %params
    cursor.execute(right_region_new)
    
    region = "CREATE TABLE Region as(SELECT left_region.accn, left_region.sfeat , left_region.left_gene, left_region.left_gene_qfeat , left_region.start as Lstart, left_region.end as Lend, \
                                     right_region.right_gene, right_region.right_gene_qfeat, right_region.start as Rstart, right_region.end as Rend, right_region.chr as seqid, right_region.strand as rstrand, left_region.strand as lstrand\
                                     FROM left_region, right_region\
                                     WHERE left_region.sfeat = right_region.sfeat AND left_region.chr = right_region.chr)"
    print region
    cursor.execute(region)

def orintation(cursor):
    same_strand = "CREATE TABLE same_strand as(SELECT Region.accn, Region.sfeat, Region.Right_gene , Region.Right_gene_qfeat,  Region.left_gene, Region.left_gene_qfeat, Region.Lend as start, region.Rstart as end , \
                                              (region.Rstart - Region.Lend ) as diff,  Region.seqid  \
                                               FROM Region \
                                               WHERE (region.Rstart > Region.Lend ))"
    cursor.execute(same_strand)
    
    opp_strand = "CREATE TABLE opp_strand as(SELECT Region.accn, Region.sfeat, Region.Right_gene , Region.Right_gene_qfeat, Region.left_gene, Region.left_gene_qfeat, Region.Rend as start, region.Lstart as end , \
                                            (region.Lend - Region.Rstart) as diff,  Region.seqid \
                                            FROM Region \
                                            WHERE (region.Rstart < Region.Lend ))"
    cursor.execute(opp_strand)

def create_final_table(org1_org2, cursor):
    """creates the final table outline, then inserts regions under 100000 bps from both the opp and same strand"""
    
    create_table = "CREATE TABLE IF NOT EXISTS FINAL (accn INTEGER , sfeat Varchar(1000), sleft_gene Varchar(1000),\
                    sright_gene Varchar(1000), sstart INTEGER , send INTEGER, sdiff INTEGER, qleft_gene Varchar(1000), qright_gene Varchar(1000), \
                    start INTEGER, end INTEGER, diff INTEGER, seqid Varchar(10), orientation Varchar(100), url Varchar(10000))"
    cursor.execute(create_table)
    
    small_regions_same ="INSERT INTO FINAL (accn , sfeat, sleft_gene, sright_gene ,qleft_gene, qright_gene , start , end , diff, seqid, orientation) \
                        SELECT same_strand.accn, same_strand.sfeat, same_strand.left_gene, same_strand.right_gene, \
                        same_strand.left_gene_qfeat, same_strand.right_gene_qfeat, same_strand.start, same_strand.end, same_strand.diff, \
                       same_strand.seqid, 'same' FROM same_strand WHERE same_strand.diff < 100000"
    cursor.execute(small_regions_same)
    
    small_regions_opp = "INSERT INTO FINAL (accn , sfeat, sleft_gene, sright_gene , qleft_gene, qright_gene , start , end , diff, seqid, orientation) \
                        SELECT opp_strand.accn, opp_strand.sfeat, opp_strand.left_gene, opp_strand.right_gene, \
                        opp_strand.left_gene_qfeat ,opp_strand.right_gene_qfeat, opp_strand.start, opp_strand.end, opp_strand.diff, \
                         opp_strand.seqid, 'opp' FROM opp_strand WHERE opp_strand.diff < 100000"
    cursor.execute(small_regions_opp)
    
    sgenes_info = "UPDATE FINAL, merge_genes SET FINAL.sdiff = merge_genes.sdiff, sstart = merge_genes.left_end , send =  merge_genes.right_start \
                   WHERE merge_genes.sfeat = FINAL.sfeat"
    cursor.execute(sgenes_info)
    
    url('FINAL', org1_org2, cursor)

def url(tablename, syn_map2, cursor, base ="http://synteny.cnr.berkeley.edu/CoGe/GEvo.pl?"):
    "creates url for each sfeat, matching the homelogous regions and inserts into mysql"
    cursor.execute("SELECT * FROM  %s " %tablename)
    region_dict = cursor.fetchall()
    for d in region_dict:
        if d['orientation'] == 'same':
            sfeat = d['sfeat']
            accn1 = d['qleft_gene'] 
            d['diff'] += 10000
            accn2 = d['sleft_gene'] 
            d['sdiff'] += 10000
            d['qfeat'] = grab_qfeat(sfeat, syn_map2, cursor)
            url = "drup1=1000&drdown1=%(diff)s&drup2=1000&drdown2=%(sdiff)s;accn1=%(qleft_gene)s;accn2=%(sleft_gene)s;accn3=%(qfeat)s;dr3up=10000;dr3down=10000;num_seqs=3"
            d['url'] = base + url % d
            import_url_to_mysql(tablename , d['url'], sfeat, cursor)
        elif d['orientation'] == 'opp': 
            sfeat = d['sfeat']
            accn1 = d['qleft_gene'] 
            d['diff'] += 10000
            accn2 = d['sleft_gene'] 
            d['sdiff'] += 10000
            d['qfeat'] = grab_qfeat(sfeat, syn_map2, cursor)
            url = "drup1=%(diff)s&drdown1=1000&drup2=1000&drdown2=%(sdiff)s;accn1=%(qleft_gene)s;accn2=%(sleft_gene)s;accn3=%(qfeat)s;dr3up=10000;dr3down=10000;num_seqs=3"
            d['url'] = base + url % d
            import_url_to_mysql(tablename , d['url'], sfeat, cursor)

def grab_qfeat(accn, syn_map2, cursor):
    "grabs the sorg qfeat for the third panell"
    params = {'accn' : accn , 'syn_map2': syn_map2}
    stmt = "SELECT %(syn_map2)s.qfeat \
            FROM %(syn_map2)s \
            WHERE '%(accn)s' = %(syn_map2)s.sfeat" %params
            
    print stmt
    cursor.execute(stmt) 
    qfeat_dict = cursor.fetchone()
    return qfeat_dict['qfeat']

def import_url_to_mysql(tablename, url, sfeat, cursor):
    stmt = "UPDATE {0} SET url = '{1}' WHERE sfeat = '{2}'".format(tablename, url, sfeat)
    print stmt
    cursor.execute(stmt)

def main(db_name, org1_org1, org1_org1_path, org1_org2, org1_org2_path, bed_path):
    """runs main function"""
    
    db=MySQLdb.connect(host="127.0.0.1", user="root", db= db_name)
    cursor = db.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    
    # synmap1 = mysql_syn_import(db_name, org1_org1)
    # synmap1.org1_org1(org1_org1_path)
    # synmap1.import_bed_to_mysql(bed_path , org1_org1_path)
    # synmap2 = mysql_syn_import(db_name, org1_org2)
    # synmap2.org1_org2(org1_org2_path)
    # synmap2.import_bed_to_mysql(bed_path , org1_org2_path)
    #create_left_right_tables('right_genes', cursor)
    #create_left_right_tables('left_genes', cursor)
    org1_org2_bed_table = '%s_bed' %org1_org2 
    org1_org1_bed_table = '%s_bed' %org1_org1
    # remove_retined_homologs(org1_org1_bed_table, org1_org2_bed_table, cursor)
    #find_left_right_gene(org1_org1, org1_org1_bed_table, cursor)
    #create_region_table(org1_org1_bed_table , org1_org1, cursor)
    orintation(cursor)
    create_final_table(org1_org2, cursor)
    
    

main('find_homeo_1', 'rice_rice', '/Users/gturco/results/data/rice_v6_rice_v6.pairs.txt', 'rice_sorg', '/Users/gturco/results/data/rice_v6_sorghum_v1.pairs.txt', '/Users/gturco/results/data/rice_v6.bed')