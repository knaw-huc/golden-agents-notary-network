import json
import xmltodict

from datetime import datetime, timedelta
from dateutil import parser

from dataclasses import dataclass


@dataclass
class Collection:
    id: str
    title: str
    description: str
    author: str
    publisher: str
    date: str

    collection_id: str
    collectionNumber: str
    collectionName: str
    collectionDate: str
    collectionLanguage: str
    collectionRepository: str
    collectionOrigination: str
    # collectionCorporation: str

    children: list


@dataclass(order=True)
class C:
    id: str
    code: str
    date: dict
    title: str
    comment: str
    scans: list

    children: list

    level: str


def parseEAD(xmlfile):
    with open(xmlfile, 'rb') as xmlrbfile:
        parse = xmltodict.parse(xmlrbfile,
                                force_list={'note', 'c'},
                                dict_constructor=dict)
        ead = parse['ead']

    collection = parseCollection(ead)

    return collection


def parseDsc(serie, parentElement=None):

    did = serie['did']

    id = did['unitid']['@identifier']
    code = did['unitid']['#text']
    date = did.get('unitdate')

    if date:
        date = parseDate(date['@normal'])

    title = did.get('unittitle', "")
    if '#text' in title:
        title = title['#text']

    comment = ""
    scans = []

    if serie['@level'] == 'file':  # reached the end!
        if 'note' in did:
            for note in did['note']:
                if note['@label'] == "NB":
                    comment = note['p']
                elif note['@label'] == "ImageId":
                    scans = note['p'].split(' \n')

        return C(id,
                 code,
                 date,
                 title,
                 comment,
                 scans, [],
                 level=serie['@level'])

    else:
        children = []
        for k in serie:
            if k not in ['head', '@level', 'did']:
                for subelement in serie[k]:
                    if type(subelement) != str:
                        children.append(parseDsc(subelement))

        return C(id, code, date, title, comment, scans, children,
                 serie['@level'])


def parseCollection(ead):

    head = ead['eadheader']
    archdesc = ead['archdesc']

    collection = Collection(
        id=head['eadid']['@identifier'],
        title=head['filedesc']['titlestmt']['titleproper'],
        description=archdesc['did']['abstract']['#text']
        if archdesc['did'].get('abstract') else "",
        author=head['filedesc']['titlestmt'].get('author'),
        publisher=head['filedesc']['publicationstmt']['publisher'],
        date=archdesc['did']['unitdate'].get('@normal'),
        collection_id=archdesc['did']['@id'],
        collectionNumber=archdesc['did']['unitid']['#text'],
        collectionName=archdesc['did']['unittitle']['#text'],
        collectionDate=archdesc['did']['unitdate']['#text'],
        collectionLanguage=archdesc['did']['langmaterial'],
        collectionRepository=archdesc['did']['repository']['corpname'],
        collectionOrigination=archdesc['did']['origination'],
        # collectionCorporation=archdesc['did']['origination']['corpname'],
        children=[parseDsc(serie) for serie in archdesc['dsc']['c']])

    return collection


def parseDate(date,
              circa=None,
              default=None,
              defaultBegin=datetime(2100, 1, 1),
              defaultEnd=datetime(2100, 12, 31)):

    if date is None or date == 's.d.':
        return {}

    date = date.strip()

    if '/' in date:
        begin, end = date.split('/')

        begin = parseDate(begin, default=defaultBegin)
        end = parseDate(end, default=defaultEnd)
    elif date.count('-') == 1:
        begin, end = date.split('-')

        begin = parseDate(begin, default=defaultBegin)
        end = parseDate(end, default=defaultEnd)
    elif 'ca.' in date:
        date, _ = date.split('ca.')

        begin = parseDate(date, default=defaultBegin, circa=365)
        end = parseDate(date, default=defaultEnd, circa=365)

    else:  # exact date ?

        if circa:
            begin = parser.parse(date, default=defaultBegin) - timedelta(circa)
            end = parser.parse(date, default=defaultEnd) + timedelta(circa)
        else:
            begin = parser.parse(date, default=defaultBegin)
            end = parser.parse(date, default=defaultEnd)

    begin = begin
    end = end

    # And now some sem magic

    if begin == end:
        timeStamp = begin
    else:
        timeStamp = None

    if type(begin) == tuple:
        earliestBeginTimeStamp = begin[0]
        latestBeginTimeStamp = begin[1]
        beginTimeStamp = None
        timeStamp = None
    else:
        earliestBeginTimeStamp = begin
        latestBeginTimeStamp = begin
        beginTimeStamp = begin

    if type(end) == tuple:
        earliestEndTimeStamp = end[0]
        latestEndTimeStamp = end[1]
        endTimeStamp = None
    else:
        earliestEndTimeStamp = end
        latestEndTimeStamp = end
        endTimeStamp = end

    if default:
        if type(begin) == tuple:
            begin = min(begin)
        if type(end) == tuple:
            end = max(end)
        return begin, end

    dt = {
        "hasTimeStamp": timeStamp,
        "hasBeginTimeStamp": beginTimeStamp,
        "hasEarliestBeginTimeStamp": earliestBeginTimeStamp,
        "hasLatestBeginTimeStamp": latestBeginTimeStamp,
        "hasEndTimeStamp": endTimeStamp,
        "hasEarliestEndTimeStamp": earliestEndTimeStamp,
        "hasLatestEndTimeStamp": latestEndTimeStamp
    }

    return dt


if __name__ == '__main__':

    ead = parseEAD(
        '5075.ead.xml')  # https://archief.amsterdam/archives/xml/5075.ead.xml

    data = dict()

    for c in ead.children:
        data[c.code] = {
            'notaris': c.title,
            'code': c.code,
            'uri': f"https://archief.amsterdam/inventarissen/file/{c.id}",
            'inventories': dict()
        }

        codes = []
        inventories = []

        if getattr(c, 'children'):
            for c2 in c.children:
                if not getattr(c2, 'children'):

                    inventories.append(
                        f"https://archief.amsterdam/inventarissen/file/{c2.id}"
                    )
                    codes.append(c2.code)
                else:
                    for c3 in c2.children:
                        if not getattr(c3, 'children'):

                            inventories.append(
                                f"https://archief.amsterdam/inventarissen/file/{c3.id}"
                            )
                            codes.append(c3.code)
                        else:
                            for c4 in c3.children:
                                if not getattr(c4, 'children'):

                                    inventories.append(
                                        f"https://archief.amsterdam/inventarissen/file/{c4.id}"
                                    )
                                    codes.append(c4.code)

        data[c.code]['codes'] = codes
        data[c.code]['inventories'] = inventories

    with open('data/notarissenEAD.json', 'w') as outfile:
        json.dump(data, outfile)