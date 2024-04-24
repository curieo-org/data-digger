import re
from lxml import etree
import boto3
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class JatsXMLParser:
    """
    The JatsXMLParser class is designed to parse and convert JATS XML files into a more structured format,
    suitable for data mining and analysis. It provides several methods for retrieving specific information from the input XML,
    such as publication dates, keywords, and author affiliations.
    """
	
    def get_file_content(self, name):
        f = open(name,'r')
        f_text = f.read()
        f.close()
        return f_text
	
    def cleanup_input_xml(self, xmlstr):
        pos = xmlstr.index("<article")
        xmlstr = xmlstr[pos:]

        pos = xmlstr.rindex("</article>") + 10
        xmlstr = xmlstr[0:pos]
        xmlstr = re.sub('xmlns = "\S*?"', '', xmlstr)
        xmlstr = xmlstr.replace("-->","--> ")

        return xmlstr
	
    def get_cardinality(self, n):
        if n>1: return 'N'
        return str(n)

    # helper function, used for stats printing
    def get_el_cardinality(self, someroot, somepath):
        c = self.get_cardinality(len(someroot.xpath(somepath)))
        return somepath + ':' + c

    # helper function, used for stats printing
    def get_stats(self, fname, someroot):
        line = 'pam-stats' + '\t'
        line += fname + '\t'
        line += self.get_el_cardinality(someroot,'/article/front/article-meta/abstract') + '\t'
        line += self.get_el_cardinality(someroot,'/article/body/p') + '\t'
        line += self.get_el_cardinality(someroot,'/article/body/sec')
        return line
	
    # helper function, used for stats printing
    # lists the tags of elements containing a <fig> for a given file
    def get_fig_parents(self, fname, someroot):
        parents = {}
        figs = someroot.xpath('/article/body//fig')
        if figs is not None:
            for fig in figs:
                parent_tag = fig.getparent().tag
                if parents.get(parent_tag) is None: parents[parent_tag] = 0
                parents[parent_tag] = parents[parent_tag]+1
        lines = []
        for p in parents:
            line = 'fig-stats' + '\t' + fname + '\t<' + p + '>:' + str(parents[p])
            lines.append(line)
        return lines

    # helper function, used for stats printing
    # lists the tags of elements containing a <table-wrap> for a given file
    def get_tw_parents(self, fname, someroot):
        parents = {}
        tws = someroot.xpath('/article/body//table-wrap')
        if tws is not None:
            for tw in tws:
                parent_tag = tw.getparent().tag
                if parents.get(parent_tag) is None: parents[parent_tag] = 0
                parents[parent_tag] = parents[parent_tag]+1
        lines = []
        for p in parents:
            line = 'tw-stats' + '\t' + fname + '\t<' + p + '>:' + str(parents[p])
            lines.append(line)
        return lines
	
    # helper function, used for stats printing
    # lists the tags of elements that are direct children of <body>
    def get_body_structure(self, fname, someroot):
        line = 'pam-struc' + '\t'
        line += fname + '\t'
        atype = someroot.xpath('/article')[0].get('article-type')
        line += atype + '\t'
        myroots = someroot.xpath('/article/body')
        if len(myroots)>0:
            myroot = myroots[0]
            for el in myroot.iterchildren():
                if isinstance(el, etree._Comment): continue
                line += el.tag + ','
        return line

    def get_keywords(self, someroot):
        kwd_list = someroot.xpath('/article//kwd')
        if kwd_list is None: return []
        result = []
        for k in kwd_list:
            result.append(self.clean_string(' '.join(k.itertext())))
        return result

    def get_multiple_texts_from_xpath(self, someroot, somepath, withErrorOnNoValue):
        result = ''
        x = someroot.xpath(somepath)
        for el in x: result += ' '.join(el.itertext())
        if len(result) >= 1:
            result = self.clean_string(result)
        elif withErrorOnNoValue:
            self.file_status_add_error("ERROR, no text for element: " + somepath)
        return result

    def get_text_from_xpath(self, someroot, somepath, withWarningOnMultipleValues, withErrorOnNoValue):
        result = ''
        x = someroot.xpath(somepath)
        if len(x) >= 1:
            result = self.get_clean_text(x[0])
            #result = x[0].text
            if len(x) > 1 and withWarningOnMultipleValues is True :
                self.file_status_add_error('WARNING: multiple elements found: ' + somepath)
        elif withErrorOnNoValue is True:
            self.file_status_add_error("ERROR, no text for element: " + somepath)
        return result

    def get_pub_date_by_type(self, someroot, selector, pubtype, format):

        if not pubtype is None: selector += '[@pub-type = "' + pubtype + '"]'
        dates = someroot.xpath(selector);
        if len(dates) == 0: return {'date': None, 'status':'not found'}
        dt = dates[0]

        status = 'ok'
        ynode = dt.find('year')
        year = ynode.text if ynode is not None and ynode.text is not None else ''
        year = year.strip()
        if len(year) == 0: return {'date': None, 'status': 'incomplete'}
        mnode = dt.find('month')
        mm = '01'
        if mnode is not None and mnode.text is not None:
            mm = mnode.text
            mm = mm.strip()
            if len(mm) == 1: mm = "0" + mm
        else:
            status = 'incomplete'
        mmm_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        mmm = ''
        if mm.isdigit() and int(mm)>0 and int(mm)<= 12:
            mmm = mmm_names[int(mm)-1]
        else:
            status = 'unparseable'
        dnode = dt.find('day')
        day = '01'
        if dnode is not None and dnode.text is not None:
            day = dnode.text
            day = day.strip()
            if len(day) == 1: day = "0" + day
        else:
            status = 'incomplete'

        formatted_date = year + ' ' + mmm + ' ' + day # default format
        if format == 'yyyy': formatted_date = year
        if format == 'd-M-yyyy': formatted_date = day + '-' + mm + '-' + year
        return {'date': formatted_date, 'status': status}
        
    def get_first_pub_date(self, someroot, format):
        selector = '/article/front/article-meta/pub-date'
        return self.get_pub_date_by_type(someroot, selector, None, format)

    def get_pub_date(self, someroot, format):
        selector = '/article/front/article-meta/pub-date'
        dt = self.get_pub_date_by_type(someroot, selector, 'epub', format)
        if dt['status'] !=  'ok': dt = self.get_pub_date_by_type(someroot, selector, 'ppub', format)
        if dt['status'] !=  'ok': dt = self.get_pub_date_by_type(someroot, selector, 'collection', format)
        if dt['status'] !=  'ok': dt = self.get_pub_date_by_type(someroot, selector, None, format)
        if dt['status'] !=  'ok': self.file_status_add_error('ERROR, element not found: ' + selector)
        return dt

    def get_pmc_release_date(self, someroot, format):
        selector = '/article/front/article-meta/pub-date'
        dt = self.get_pub_date_by_type(someroot, selector, 'pmc-release', format)
        if dt['status'] !=  'ok':
            return {'date': None, 'status': "ok"}
        return dt

    def build_medlinePgn(self, fp, lp):
        if fp != None and len(fp)>0 and lp != None and len(lp)>0: return fp + '-' + lp
        if fp != None and len(fp)>0: return fp + '-?'
        if lp != None and len(lp)>0: return '?-' + lp
        return ''

    def get_affiliations(self, someroot):
        result = []
        affs = someroot.xpath('/article/front/article-meta//aff')
        for aff in affs:
            id = aff.get('id')
            # extract label text and then remove node
            label_node = aff.find('label')
            label = self.get_clean_text(label_node)
            if label_node is not None: label_node.text = ''
            institution = self.get_clean_text(aff.find('institution'))
            country = self.get_clean_text(aff.find('country'))
            if len(institution)>0 and len(country)>0:
                name = institution + ', ' + country
            # otherwise build name from any text found in there
            else:
                name = self.get_clean_text(aff)

            result.append({'id':id, 'label':label, 'name': self.clean_string(name)})
        return result

    def get_authors(self, someroot):
        authors = someroot.xpath('/article/front/article-meta/contrib-group/contrib[@contrib-type = "author"]')
        result = []
        for a in authors:
            surname = ''
            givennames = ''
            affiliation_list = []
            for el in a.iter():
                if el.tag ==  'surname':
                    if el.text !=  None: surname = self.clean_string(el.text)
                elif el.tag ==  'given-names':
                    if el.text !=  None: givennames = self.clean_string(el.text)
                # affiliations
                elif el.tag ==  'xref' and el.get('ref-type') == 'aff':
                    if el.get('rid') !=  None: affiliation_list.append(el.get('rid'))
                # affiliations (alternative)
                elif el.tag ==  'aff':
                    if el.text !=  None: affiliation_list.append(self.clean_string(el.text))

            author = {}
            author['affiliations'] = affiliation_list
            author['last_name'] = surname
            author['first_name'] = givennames
            author['name'] = (givennames + ' ' + surname).strip()
            author['initials'] = self.get_initials(givennames)
            result.append(author)
        if len(result) == 0: self.file_status_add_error("WARNING: no authors")
        return result

    def get_initials(self, multiple_names):
        if multiple_names == '': return ''
        names = multiple_names.split(' ')
        initials = ''
        for name in names:
            if len(name.strip()) > 0: initials += name[0]
        return initials

    def clean_string(self, s1):
        if s1 is None: return None
        s2 = s1.replace('\n', ' ').replace(u'\u00a0', ' ').replace('\t', ' ').strip()
        return ' '.join(s2.split())

    def get_abstract(self, someroot):
        x = someroot.xpath('/article/front/article-meta/abstract')
        content = ''
        for xi in x:
            content += ' '.join(xi.itertext()) + ' '
        return self.clean_string(content)

    def indent(self, level):
        spaces = ''
        for i in range(1,level): spaces += '  '
        return spaces

    def coalesce(self, *arg):
        for el in arg:
            if el is not None:
                return el
        return None

    def handle_boxed_text_elements(self, someroot):
        bt_list = someroot.xpath('//boxed-text')
        if bt_list is None: return
        if bt_list == []: return
        for bt in bt_list: bt.getparent().remove(bt)
        self.file_status_add_error('WARNING: removed some <boxed-text> element(s)')

    def remove_alternative_title_if_redundant(self, someroot):
        ttl = someroot.find('./front/article-meta/title-group/article-title')
        alt = someroot.find('./front/article-meta/title-group/alt-title')
        if ttl is not  None and alt is not None: alt.getparent().remove(alt)

    # we remove all elements and their subtree having tag in tag_list
    def remove_subtree_of_elements(self, someroot, tag_list):
        el_list = someroot.iter(tag_list)
        for el in el_list: el.getparent().remove(el)

    def handle_table_wrap(self, pmcid, tw):
        xref_id = tw.get('id') or ''
        xref_url = 'https://www.ncbi.nlm.nih.gov/pmc/articles/' + pmcid + '/table/' + xref_id
        label = self.get_clean_text(tw.find('label'))
        caption = self.get_clean_text(tw.find('caption'))
        footer  = self.get_clean_text(tw.xpath('table-wrap-foot'))
        media_hrefs = [ self.get_xlink_href(el) for el in tw.xpath('media') ]
        graph_hrefs = [ self.get_xlink_href(el) for el in tw.xpath('graphic') ]
        # table content
        columns = []
        row_values = []
        table_xml = b''
        table_tree = tw.find('table')
        if table_tree is None: table_tree = tw.find('alternatives/table')
        if table_tree is not None:
            table_xml = etree.tostring(table_tree)
            columns, row_values = self.table_to_df(table_xml)
        return {'tag': 'table', 'xref_id': xref_id, 'xref_url': xref_url,
                'label': label, 'caption': caption, 'footer':footer,
                'media':media_hrefs, 'graphics':graph_hrefs,
                'table_columns': columns, 'table_values': row_values,
                'xml':table_xml.decode("utf-8")}

    def table_to_df(self, table_text):
        table_tree = etree.fromstring(table_text)
        columns = []
        for tr in table_tree.xpath('thead/tr'):
            for c in tr.getchildren():
                col_content = ' '.join(c.itertext())
                col_content = col_content.strip()
                columns.append(col_content)

        row_values = []
        len_rows = []
        for tr in table_tree.findall('tbody/tr'):
            es = tr.xpath('td')
            row_value = list()
            for e in es:
                joined_e = ' '.join(e.itertext())
                clean_e = joined_e.strip()
                row_value.append(clean_e)
            len_rows.append(len(es))
            row_values.append(row_value)

        if len(len_rows) >= 1:
            return columns, row_values
        else:
            return None, None

    def get_clean_text(self, el):
        if el is None: return ''
        if type(el) ==  list:
            return self.clean_string(' '.join([' '.join(sub_el.itertext()) for sub_el in el]))
        else:
            return self.clean_string(' '.join(el.itertext()))

    def modify_insert_text_in_sub_element(self, ins_texts, subel_tag, el):
        texts = []
        texts.extend(ins_texts)
        subel = el.find(subel_tag)
        if (subel is not None): texts.append(' '.join(subel.itertext()))
        new_text = self.clean_string(' '.join(texts))
        for subel in el.iterchildren(subel_tag): el.remove(subel)
        new_subel = etree.SubElement(el, subel_tag)
        new_subel.text = new_text

    def get_xlink_href(self, el):
        if el is None: return None
        for k in el.keys():
            if k[-4:] == 'href': return el.get(k)
        return None
    
    def handle_supplementary_material_elements(self, someroot):
        etree.strip_tags(someroot,'supplementary-material')

    def handle_supplementary_material_elements_ori(self, someroot):
        sm_list = someroot.xpath('//supplementary-material')
        if sm_list is None: return
        for sm in sm_list:
            for el in sm.iterchildren('table-wrap','p','fig'):
                sm.addprevious(el)
            sm.getparent().remove(sm)

    def handle_table_wrap_group_elements(self, someroot):
        g_list = someroot.xpath('//table-wrap-group')
        if g_list is None: return
        for g in g_list:
            # store table-wrap-group caption and label
            g_captions = []
            for gc in g.iterchildren('caption'): g_captions.append(' '.join(gc.itertext()))
            g_labels = []
            for gl in g.iterchildren('label'): g_labels.append(' '.join(gl.itertext()))
            for tw in g.xpath('table-wrap'):
                self.modify_insert_text_in_sub_element(g_labels, 'label', tw)
                self.modify_insert_text_in_sub_element(g_captions, 'caption', tw)
                # moves tw as the previous sibling of table-wrap-group
                g.addprevious(tw)
            g.getparent().remove(g)

    def remove_embedding_group_elements(self, someroot, el_tag):
        g_list = someroot.xpath('//' + el_tag + '-group')
        if g_list is None: return
        for g in g_list:
            for el in g.xpath(el_tag):
                g.addprevious(el)
            g.getparent().remove(g)

    def handle_fig_group_elements(self, someroot):
        fg_list = someroot.xpath('//fig-group')
        if fg_list is None: return
        for fg in fg_list:
            # store fig-group caption
            fg_captions = []
            for fgc in fg.iterchildren('caption'): fg_captions.append(' '.join(fgc.itertext()))
            for fig in fg.xpath('fig'):
                self.modify_insert_text_in_sub_element(fg_captions, 'caption', fig)
                # moves fig as the previous sibling of fig-group
                fg.addprevious(fig)
            fg.getparent().remove(fg)

    def handle_fig(self, pmcid, fig):
        xref_id = fig.get('id') or ''
        xref_url = 'https://www.ncbi.nlm.nih.gov/pmc/articles/' + pmcid + '/figure/' + xref_id
        if xref_id ==  '': xref_url = ''
        fig_label = self.get_clean_text(fig.find('label'))
        fig_caption = self.get_clean_text(fig.find('caption'))
        media_hrefs = [ self.get_xlink_href(el) for el in fig.xpath('media') ]
        graph_hrefs = [ self.get_xlink_href(el) for el in fig.xpath('graphic') ]
        return {'tag':'fig', 'caption': fig_caption, 'xref_url': xref_url,
                'xref_id': xref_id, 'label': fig_label, 'media': media_hrefs,
                'graphics': graph_hrefs, 'pmcid':pmcid }

    def handle_list(self, list):
        contentList = []
        for el in list.iterchildren(['list-item']):
            contentList.append({'tag': 'list-item', 'text': self.clean_string(' '.join(el.itertext()))})
        tail = self.clean_string(list.tail)
        if tail is not None: contentList.append({'tag': 'p', 'text': tail})
        return contentList

    def handle_paragraph(self, pmcid, el):
        self.simplify_node(el, ['fig','table-wrap','list'])
        contentList = []
        ptext = self.clean_string(el.text)
        if ptext is not None and ptext !=  '': contentList.append({'tag':'p', 'text': ptext})
        for sub_el in el.iterchildren():
            if sub_el.tag ==  'fig':
                contentList.append(self.handle_fig(pmcid,sub_el))
            elif sub_el.tag ==  'table-wrap':
                contentList.append(self.handle_table_wrap(pmcid,sub_el))
            elif sub_el.tag ==  'list':
                contentList.extend(self.handle_list(sub_el))
            else:
                contentList.append({'tag':sub_el.tag, 'text': self.get_clean_text(sub_el)})
        ptail = self.clean_string(el.tail)
        if ptail is not None and ptail !=  '': contentList.append({'tag':'p', 'text': ptail})
        return contentList

    def handle_paragraph_old(self, pmcid, el):
        contentList = []
        for sub_el in el.iterchildren(['fig','table-wrap']):
            if sub_el.tag ==  'fig':
                contentList.append(self.handle_fig(pmcid,sub_el))
            elif sub_el.tag ==  'table-wrap':
                contentList.append(self.handle_table_wrap(pmcid,sub_el))
            sub_el.getparent().remove(sub_el)
        content = {'tag': el.tag, 'text': self.clean_string(' '.join(el.itertext()))}
        contentList.insert(0,content)
        return contentList

    def handle_section_flat(self, pmcid, sec, level, implicit):
        sectionList = []
        id = ''.join(sec.xpath('@id'))
        title = self.get_clean_text(sec.find('title'))
        caption = self.get_clean_text(sec.find('caption'))
        label = self.get_clean_text(sec.find('label'))
        mainSection = {
            'implicit':implicit,
            'level': level,
            'id': self.build_id(self.block_id),
            'title': title,
            'label': label,
            'caption': caption,
            'tag': sec.tag,
            'contents':[]
            }

        sectionList.append(mainSection)
        self.block_id.append(0)
        terminalContentShouldBeWrapped = False

        for el in sec:

            # ignore elements handled elsewhere or that are unnecessary
            if isinstance(el, etree._Comment): continue
            if isinstance(el, etree._XSLTProcessingInstruction): continue
            if isinstance(el, etree._ProcessingInstruction): continue
            if el.tag ==  'title': continue
            if el.tag ==  'label': continue
            if el.tag ==  'caption': continue

            # recursive call for any embedded section <sec>, <boxed-text> and/or <app> (appendices)
            if el.tag ==  'sec' or el.tag ==  'app' or el.tag ==  'boxed-text':
                self.block_id[-1] = self.block_id[-1] + 1
                terminalContentShouldBeWrapped = True
                sectionList.extend(self.handle_section_flat(pmcid, el, level + 1, False))
                continue

            contentsToBeAdded = []
            # handle paragraphs: will return paragraph content plus any embedded figures or tables as sibling contents
            if el.tag ==  'p':
                contentsToBeAdded = self.handle_paragraph(pmcid, el)
            elif el.tag ==  'fig':
                contentsToBeAdded = [self.handle_fig(pmcid, el)]
            elif el.tag ==  'table-wrap':
                contentsToBeAdded = [self.handle_table_wrap(pmcid, el)]
            elif el.tag ==  'list':
                contentsToBeAdded = self.handle_list(el)
            # default handler: just keep tag and get all text
            else:
                sometext = self.clean_string(' '.join(el.itertext()))
                if sometext is not None and sometext !=  '':
                    contentsToBeAdded = [ {'tag': el.tag, 'text': sometext} ]

            self.addContentsOrWrappedContents(sectionList, mainSection, contentsToBeAdded, level, terminalContentShouldBeWrapped)

        self.block_id.pop()
        return sectionList

    def addContentsOrWrappedContents(self, sectionList, currentSection, contentsToBeAdded, level, shouldBeWrapped):
        if contentsToBeAdded == []: return
        targetContents = currentSection['contents']
        if shouldBeWrapped:
            self.block_id[-1] = self.block_id[-1] + 1
            wid = self.build_id(self.block_id)
            subSection = {'implicit':True, 'level':level+1, 'id':wid, 'title':'', 'label':'' ,'tag':'wrap', 'contents':[]}
            sectionList.append(subSection)
            targetContents = subSection['contents']
            self.block_id.append(0)
        for content in contentsToBeAdded:
            self.block_id[-1] = self.block_id[-1] + 1
            content['id'] = self.build_id(self.block_id)
            targetContents.append(content)
        if shouldBeWrapped:
            self.block_id.pop()

    def build_id(self, a):
        id = ''
        for num in self.block_id: id += str(num) + '.'
        return id[0:-1]
    
    def file_status_reset(self):
        self.file_status['name'] = ''
        self.file_status['errors'].clear()

    def file_status_set_name(self, n):
        self.file_status['name'] = n

    def file_status_add_error(self, r):
        self.file_status['errors'].append(r)

    def file_status_ok(self):
        return len(self.file_status['errors']) == 0

    def file_status_print(self):
        msg = self.file_status['name'] + '\t'
        msg += str(len(self.file_status['errors'])) + '\t'
        for r in self.file_status['errors']: msg += r + '\t'
        print(msg)


    def parse_PMC_XML_core(self, root):

        self.block_id.clear()
        dict_doc = {}

        for xs in root.xpath('//xref/sup'): xs.getparent().remove(xs)
        for sx in root.xpath('//sup/xref'): sx.getparent().remove(sx)
        etree.strip_tags(root,'sup')

        etree.strip_tags(root,'italic')
        etree.strip_tags(root,'bold')
        etree.strip_tags(root,'sub')
        etree.strip_tags(root,'ext-link')

        # rename this erroneous element
        for el in root.xpath('/article/floats-wrap'): el.tag = 'floats-group'

        etree.strip_elements(root, 'inline-formula','disp-formula', with_tail = False)
        self.handle_supplementary_material_elements(root)
        self.handle_table_wrap_group_elements(root)
        self.handle_fig_group_elements(root)
        self.remove_embedding_group_elements(root,'fn')  # removes  fn-group wrapper (foot-notes)
        self.remove_embedding_group_elements(root,'app') # removes app-group wrapper (appendices)
        self.remove_alternative_title_if_redundant(root)
        # End preprocessing


        # Now retrieve data from refactored XML
        dict_doc['affiliations'] = self.get_affiliations(root)
        dict_doc['authors'] = self.get_authors(root)

        # note: we use xref to retrieve author affiliations above this line
        etree.strip_tags(root,'xref')

        dict_doc['article_type'] = root.xpath('/article')[0].get('article-type')
        lng = root.xpath('/article')[0].get('{http://www.w3.org/XML/1998/namespace}lang')
        if lng ==  None : lng = ''
        dict_doc['language'] = lng[0:2]

        # note: we can get multiple journal-id elements with different journal-id-type attributes
        dict_doc['medline_ta'] = self.get_text_from_xpath(root, '/article/front/journal-meta/journal-id', False, True)

        dict_doc['journal'] = self.get_multiple_texts_from_xpath(root, '/article/front/journal-meta//journal-title', True)
        dict_doc['title'] = self.get_multiple_texts_from_xpath(root, '/article/front/article-meta/title-group', True)
        dict_doc['pmid'] = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "pmid"]', True, False)
        dict_doc['doi'] = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "doi"]', True, False)
        dict_doc['archive_id'] = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "archive"]', True, False)
        dict_doc['manuscript_id'] = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "manuscript"]', True, False)

        pmc1 = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "pmc-uid"]', True, False)
        pmc2 = self.get_text_from_xpath(root, '/article/front/article-meta/article-id[@pub-id-type = "pmc"]', True, False)
        pmc = pmc1
        if pmc ==  '': pmc = pmc2
        if pmc ==  '' and dict_doc['archive_id'] ==  '':  self.file_status_add_error("ERROR, no value for article id in types pmc-uid, pmc, or archive")
        dict_doc['pmcid'] = pmc
        dict_doc['_id'] = pmc
        if pmc ==  '' and dict_doc['archive_id'] !=  '': dict_doc['_id'] = dict_doc['archive_id']

        dict_doc['publication_date'] = self.get_pub_date(root, 'd-M-yyyy')['date']
        dict_doc['pmc_release_date'] = self.get_pmc_release_date(root, 'd-M-yyyy')['date']
        dict_doc['pubyear'] = self.get_pub_date(root, 'yyyy')['date']

        dict_doc['issue'] = self.get_text_from_xpath(root, '/article/front/article-meta/issue', True, False)
        dict_doc['volume'] = self.get_text_from_xpath(root, '/article/front/article-meta/volume', True, False)
        fp = self.get_text_from_xpath(root, '/article/front/article-meta/fpage', False, False)
        lp = self.get_text_from_xpath(root, '/article/front/article-meta/lpage', False, False)
        dict_doc['start_page'] = fp
        dict_doc['end_page'] = lp
        dict_doc['medline_pgn'] = self.build_medlinePgn(fp,lp)
        dict_doc['abstract'] = self.get_clean_text(root.find('front/article-meta/abstract'))
        dict_doc['keywords'] = self.get_keywords(root)

        # filling body, back and floats sections
        dict_doc['body_sections'] = []
        self.block_id.append(1)

        if dict_doc['title'] !=  '':
            dict_doc['body_sections'].append({
                'implicit':True, 'level':1, 'id':'1', 'label':'', 'title':'Title',
                'contents': [{'tag':'p', 'id':'1.1', 'text': dict_doc['title']}]})
            self.block_id[-1] = self.block_id[-1] + 1

        if dict_doc['abstract'] !=  '':
            abs_node = root.find('./front/article-meta/abstract')
            abs_title = etree.SubElement(abs_node, "title")
            abs_title.text = 'Abstract'
            sectionList = self.handle_section_flat(dict_doc['_id'], abs_node, 1, False)
            dict_doc['body_sections'].extend(sectionList)
            self.block_id[-1] = self.block_id[-1] + 1

        dict_doc['body_sections'].extend(self.get_sections(dict_doc['pmcid'], root.find('body')))
        dict_doc['float_sections'] = self.get_sections(dict_doc['pmcid'], root.find('floats-group'))
        dict_doc['back_sections'] = self.get_sections(dict_doc['pmcid'], root.find('back'))

        # for stats and debugging, can be commented
        dict_doc['figures_in_body'] = len(root.xpath('/article/body//fig'))
        dict_doc['figures_in_back'] = len(root.xpath('/article/back//fig'))
        dict_doc['figures_in_float'] = len(root.xpath('/article/floats-group//fig'))
        dict_doc['tables_in_body'] = len(root.xpath('/article/body//table'))
        dict_doc['tables_in_back'] = len(root.xpath('/article/back//table'))
        dict_doc['tables_in_float'] = len(root.xpath('/article/floats-group//table'))
        dict_doc['paragraphs_in_body'] = len(root.xpath('/article/body//p'))
        dict_doc['paragraphs_in_back'] = len(root.xpath('/article/back//p'))
        dict_doc['paragraphs_in_float'] = len(root.xpath('/article/floats-group//p'))

        # for compatibility reasons
        dict_doc['pmcid'] = 'PMC' + dict_doc['pmcid']
        dict_doc['_id'] = dict_doc['pmcid']
        # in case of a preprint we only have an archive id, we store it as the _id
        if dict_doc['pmcid'] ==  'PMC'  and dict_doc['archive_id'] !=  '': dict_doc['_id'] = dict_doc['archive_id']
        # if we have no pmcid, store an empty string for it
        if dict_doc['pmcid'] ==  'PMC': dict_doc['pmcid'] = ''

        return dict_doc

    def get_sections(self, pmcid, node):
        if node is None: return []
        sections = self.handle_section_flat(pmcid, node, 1, True)
        self.block_id[-1] = self.block_id[-1] + 1
        return sections

    def simplify_node(self, node ,kept_tags):
        elems = list()
        self.recursive_simplify_node(node, kept_tags, elems)
        node_tail = node.tail
        node.clear()
        node.tail = node_tail
        trg = node
        for el in elems:
            if type(el) is str:
                if trg ==  node:
                    trg.text = el if trg.text is None else trg.text + el
                else:
                    trg.tail = el if trg.tail is None else trg.tail + el
            else:
                trg = el
                node.append(trg)

    def recursive_simplify_node(self, node ,kept_tags, elems, top_level = True):
        if node.tag in kept_tags:
            elems.append(node)
        else:
            if node.text is not None: elems.append(node.text)
            for child in node.iterchildren(): self.recursive_simplify_node(child, kept_tags, elems, False)
            if node.tail is not None and not top_level:
                top_level = False
                elems.append(node.tail)  

    def download_s3_file(self,
                        s3_bucket,
                        s3_object):
        s3 = boto3.client("s3")
    
        try:
            # Download the XML file from S3
            s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_object)
            xml_content = s3_response['Body'].read()
            return xml_content.decode("utf-8")
            
        except NoCredentialsError:
            print("Error: AWS credentials not found. Please set up your AWS credentials.")
        except PartialCredentialsError:
            print("Error: Partial AWS credentials found. Please ensure your credentials are complete.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def parse_root_node(self) -> dict:
        self.xmlstr = self.cleanup_input_xml(self.xml_data)

        self.root = etree.fromstring(self.xmlstr)
        self.parsed_dict_doc = self.parse_PMC_XML_core(
            root = self.root
            )

        return self.parsed_dict_doc


    def __init__(self,
                s3_bucket: str,
                s3_object: str):
        self.file_status = {'name':'', 'errors':[]}
        self.block_id = [] 
        self.s3_bucket = s3_bucket
        self.s3_obj = s3_object
        self.xml_data = self.download_s3_file(
            s3_bucket=self.s3_bucket,
            s3_object=self.s3_obj
        )
        self.file_status_reset()
        self.file_status_set_name(n = self.s3_obj)

        