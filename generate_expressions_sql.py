#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成expressions表和expression_pronunciations表的SQL插入语句
根据Satgwong_processed.csv文件和themes分类数据
"""

import csv
import uuid
import re
from datetime import datetime

# 主题分类映射 - 根据themes_insert.sql文件建立映射
THEME_MAPPING = {
    # 1级分类
    '一、人物': 1,
    '二、自然物和自然現象': 2,
    '三、人造物': 3,
    '四、時間與空間': 4,
    '五、心理與才能': 5,
    '六、運動與動作[包括人與動物共通的動作。動物特有的動作參見二D2]': 6,
    '七、人類活動[思想活動參見五B]': 7,
    '八、抽象事物': 8,
    '九、狀況與現象[自然現象和生理現象參見二，心理現象參見五，某些社會現象參見八]': 9,
    '十、數與量': 10,
    '十一、其他': 11,
    
    # 2级分类
    '一A泛稱': 12,
    '一B男女老少': 13,
    '一C親屬、親戚': 14,
    '一D各種體貌、狀態的人': 15,
    '一E各種社會身份、境況的人': 16,
    '一F各種職業、行當的人': 17,
    '一G各種性格、品行的人': 18,
    '二A非生物體及現象': 19,
    '二B人體[與動物身體部位通用的詞語亦收於此]': 20,
    '二C生理活動、狀態和現象[生理活動與動作通用的詞語參見，與動植物通用的詞語亦收於此]': 21,
    '二D動物': 22,
    '二E植物[作食物或藥物而經過加工的植物製品參見三B、三D10]': 23,
    '三A生活用品和設施三': 24,
    '三B食[穀物、蔬菜瓜果等參見二E]': 25,
    '三C一般工具、原料、零件等': 26,
    '三D社會各業及公共設施、用品': 27,
    '四A時間[時間的計量單位見十E1]': 28,
    '四B空間[空間、面積、位置的計量單位見十E2]': 29,
    '五A心情': 30,
    '五B思想活動與狀態': 31,
    '五C性格': 32,
    '五D品行': 33,
    '五E才智': 34,
    '六A泛指的運動': 35,
    '六B軀體動作': 36,
    '六C五官動作': 37,
    '六D四肢動作': 38,
    '七A一般活動': 39,
    '七B日常生活': 40,
    '七C言語活動': 41,
    '七D職業性活動': 42,
    '七E其他社會活動': 43,
    '八A事情、外貌': 44,
    '八B意識與能力': 45,
    '八C社會性事物': 46,
    '九A外形與外貌': 47,
    '九B物體狀態': 48,
    '九C境況與表現': 49,
    '九D性質與事態': 50,
    '十A數量': 51,
    '十B人與動植物的計量單位': 52,
    '十C物體的計量單位[表示容器的名詞一般都可以作量詞，此處不一一列出，參見三]': 53,
    '十D貨幣和度量衡單位': 54,
    '十E時間和空間的計量單位': 55,
    '十F抽象的計量單位': 56,
    '十一A語氣': 57,
    '十一B摹擬聲響': 58,
    '十一C熟語[在前面各類中已有不少熟語，本節為舉例性質]': 59,
    
    # 3级分类
    '一A1人稱、指代': 60,
    '一A2一般指稱、尊稱': 61,
    '一A3詈稱、貶稱': 62,
    '一B1孩子、男孩子、青少年': 63,
    '一B2女孩子、女青年': 64,
    '一B3成年人、成年男性': 65,
    '一B4成年女性': 66,
    '一C1父母輩': 67,
    '一C2祖輩、曾祖輩': 68,
    '一C3同輩': 69,
    '一C4後輩': 70,
    '一C5家人合稱': 71,
    '一C6其他': 72,
    '一D1各種體形的人': 73,
    '一D2各種外貌的人': 74,
    '一D3各種身體狀況的人': 75,
    '一D4各種精神狀態的人': 76,
    '一D5其他': 77,
    '一E1東家、雇工、顧客': 78,
    '一E2朋友、合作者、鄰里、情人、婚嫁人物': 79,
    '一E3能人、內行人、有權勢者': 80,
    '一E4不幸者、尷尬者、生手': 81,
    '一E5鰥寡孤獨': 82,
    '一E6外地人、海外華人': 83,
    '一E7外國人': 84,
    '一E8其他': 85,
    '一F1工人、機械人員': 86,
    '一F2農民': 87,
    '一F3軍人、員警兵士': 88,
    '一F4教育、文藝、體育界人員': 89,
    '一F5商人、服務人員': 90,
    '一F6無正當職業者': 91,
    '一F7其他': 92,
    '一G1好人': 93,
    '一G2聰明人、老成人': 94,
    '一G3各種性格的人': 95,
    '一G4愚笨的人、糊塗的人': 96,
    '一G5蠻橫的人、難調教的人': 97,
    '一G6有各種不良習氣的人': 98,
    '一G7壞人、品質差的人': 99,
    '一G8其他': 100,
    '二A1日、月、星、雲': 101,
    '二A2地貌、水文、泥土、石頭': 102,
    '二A3氣象、氣候': 103,
    '二A4灰塵、污跡、霧氣、氣味': 104,
    '二A5水、水泡、火、火灰': 105,
    '二A6其他': 106,
    '二B1頭頸部': 107,
    '二B2五官、口腔、咽喉部': 108,
    '二B3軀體': 109,
    '二B4四肢': 110,
    '二B5排泄物、分泌物': 111,
    '二B6其他': 112,
    '二C1生與死': 113,
    '二C2年少、年老': 114,
    '二C3性交、懷孕、生育': 115,
    '二C4餓、飽、渴、饞': 116,
    '二C5睏、睡、醉、醒': 117,
    '二C6呼吸': 118,
    '二C7感覺[對食物的感覺參見八]': 119,
    '二C8排泄': 120,
    '二C9健康、力大、體弱、患病、痊癒': 121,
    '二C10症狀': 122,
    '二C11損傷、疤痕': 123,
    '二C12體表疾患': 124,
    '二C13體內疾患（含扭傷）': 125,
    '二C14精神病': 126,
    '二C15殘疾、生理缺陷': 127,
    '二C16其他': 128,
    '二D1與動物有關的名物[與人類共通的身體部位參見二B，作食物而分解的動物部位參見三B]': 129,
    '二D2動物的動作和生理現象[與人類共通的動作和生理現象參見六B、六C及二C]': 130,
    '二D3家畜、家禽、狗、貓': 131,
    '二D4獸類、鼠類、野生食草動物': 132,
    '二D5鳥類': 133,
    '二D6蟲類': 134,
    '二D7爬行類': 135,
    '二D8兩栖類': 136,
    '二D9淡水魚類': 137,
    '二D10海水魚類': 138,
    '二D11蝦、蟹': 139,
    '二D12軟體動物（含貝殼類）、腔腸動物': 140,
    '二E1與植物有關的名物和現象': 141,
    '二E2穀物': 142,
    '二E3水果、乾果': 143,
    '二E4莖葉類蔬菜': 144,
    '二E5瓜類、豆類、茄果類食用植物': 145,
    '二E6塊莖類食用植物': 146,
    '二E7花、草、竹、樹': 147,
    '二E8其他[附微生物]': 148,
    '三A1衣、褲、裙': 149,
    '三A2其他衣物、鞋、帽': 150,
    '三A3衣物各部位及有關名稱': 151,
    '三A4床上用品': 152,
    '三A5飾物、化妝品': 153,
    '三A6鐘錶、眼鏡、照相器材': 154,
    '三A7紙類': 155,
    '三A8自行車及其零部件、有關用具[與其他車類通用者均列於此]': 156,
    '三A9衛生和清潔用品、用具': 157,
    '三A10一般器皿、盛器、盛具': 158,
    '三A11廚具、食具、茶具': 159,
    '三A12燃具、燃料': 160,
    '三A13傢俱及有關器物': 161,
    '三A14家用電器、音響設備': 162,
    '三A15用電設施、水暖設施': 163,
    '三A16文具、書報': 164,
    '三A17通郵、電訊用品': 165,
    '三A18其他日用品': 166,
    '三A19娛樂品、玩具': 167,
    '三A20喜慶用品': 168,
    '三A21喪葬品、祭奠品、喪葬場所': 169,
    '三A22菸、毒品': 170,
    '三A23證明文件等': 171,
    '三A24貨幣[貨幣單位參見十D1]': 172,
    '三A25住宅': 173,
    '三A26廢棄物': 174,
    '三B1畜肉[與其他肉類共通的名稱亦列於此]': 175,
    '三B2禽肉、水產品肉類': 176,
    '三B3米、素食的半製成品': 177,
    '三B4葷食的半製成品': 178,
    '三B5飯食': 179,
    '三B6菜肴': 180,
    '三B7中式點心': 181,
    '三B8西式點心': 182,
    '三B9調味品、食品添加劑': 183,
    '三B10飲料': 184,
    '三B11零食、小吃': 185,
    '三C1一般工具': 186,
    '三C2金屬、塑膠、橡膠、石油製品': 187,
    '三C3機器及零件等': 188,
    '三C4其他': 189,
    '三D1農副業、水利設施及用品': 190,
    '三D2車輛及其部件[自行車及與之通用的部件參見三A8]': 191,
    '三D3船隻及其部件、飛機': 192,
    '三D4交通設施': 193,
    '三D5建築用具、材料及場所': 194,
    '三D6建築物及其構件': 195,
    '三D7布料、製衣用具': 196,
    '三D8傢俱製造用料': 197,
    '三D9體育用品、樂器': 198,
    '三D10醫療設施、藥物、場所': 199,
    '三D11商店、交易場所、商業用品': 200,
    '三D12飲食、服務、娛樂場所及用品': 201,
    '三D13軍警裝備及設施、民用槍械': 202,
    '三D14其他生產用品與產品': 203,
    '三D15其他器物及場所': 204,
    '四A1以前、現在、以後': 205,
    '四A2最初、剛才、後來': 206,
    '四A3白天、晚上': 207,
    '四A4昨天、今天、明天': 208,
    '四A5去年、今年、明年': 209,
    '四A6時節、時令': 210,
    '四A7時刻、時段': 211,
    '四A8這時、那時、早些時': 212,
    '四A9其他': 213,
    '四B1地方、處所、位置、方位': 214,
    '四B2上下、底面': 215,
    '四B3前後左右、旁邊、中間、附近': 216,
    '四B4內外': 217,
    '四B5排列位置': 218,
    '四B6到處': 219,
    '四B7邊角孔縫': 220,
    '四B8地段': 221,
    '四B9地區': 222,
    '四B10其他': 223,
    '五A1高興、興奮、安心': 224,
    '五A2憂愁、憋氣、頭痛、操心': 225,
    '五A3生氣、煩躁[發脾氣參見七E22]': 226,
    '五A4害怕、害羞': 227,
    '五A5鎮定、緊張、著急': 228,
    '五A6掛念、擔心、放心': 229,
    '五A7其他': 230,
    '五B1思考、回憶': 231,
    '五B2猜想、估計': 232,
    '五B3低估、輕視、誤會、想不到': 233,
    '五B4專心、留意、小心': 234,
    '五B5分心、不留意、無心': 235,
    '五B6喜歡、心疼、憎惡、忌諱': 236,
    '五B7願意、盼望、羡慕、妒忌、打算、故意': 237,
    '五B8有耐心、下決心、沒耐心、猶豫': 238,
    '五B9服氣、不服氣、後悔、無悔': 239,
    '五B10知道、明白、懂得、領悟': 240,
    '五B11不知道、糊塗、閉塞': 241,
    '五B12膽大、膽小': 242,
    '五B13其他': 243,
    '五C1和善、爽朗': 244,
    '五C2軟弱、小氣、慢性子': 245,
    '五C3脾氣壞、倔強、固執、淘氣': 246,
    '五C4愛多事、嘮叨、挑別': 247,
    '五C5文靜、內向': 248,
    '五C6其他': 249,
    '五D1善良、忠厚、講信用、高貴': 250,
    '五D2勤奮、懂事、老成': 251,
    '五D3不懂事、健忘、粗心': 252,
    '五D4高傲、輕浮': 253,
    '五D5自私、吝嗇、貪心、懶惰': 254,
    '五D6奸詐、缺德、負義、下賤、淫蕩': 255,
    '五D7蠻橫、粗野[霸道參見七E20]': 256,
    '五E1有文化、聰明、能幹、狡猾、滑頭': 257,
    '五E2愚笨、無能、見識少': 258,
    '五E3會說不會做': 259,
    '五E4其他': 260,
    '六A1泛指的運動': 261,
    '六A2趨向運動': 262,
    '六A3液體的運動': 263,
    '六A4搖擺、晃動、抖動': 264,
    '六A5轉動、滾動': 265,
    '六A6掉下、滑下、塌下': 266,
    '六A7其他': 267,
    '六B1軀幹部位動作': 268,
    '六B2全身動作': 269,
    '六B3頭部動作': 270,
    '六B4被動性動作、發抖': 271,
    '六C1眼部動作': 272,
    '六C2嘴部（含牙、舌）動作、鼻部動作[說話參見七C]': 273,
    '六D1拿、抓、提等': 274,
    '六D2推、拉、按、托、捏等': 275,
    '六D3扔、搖、翻開、抖開等': 276,
    '六D4捶、敲、抽打等': 277,
    '六D5放、壘、墊、塞等': 278,
    '六D6揭、摺、撕、揉、挖等': 279,
    '六D7裝、蓋、綁、聯結等': 280,
    '六D8砍、削、戳、碾等': 281,
    '六D9洗、舀、倒、拌等': 282,
    '六D10其他手部動作': 283,
    '六D11腿部動作': 284,
    '六D12其他': 285,
    '七A1過日子': 286,
    '七A2做事、擺弄、料理、安排': 287,
    '七A3領頭、主管、負責': 288,
    '七A4完成、收尾': 289,
    '七A5成功、走運、碰運氣': 290,
    '七A6得益、漁利': 291,
    '七A7失敗、出錯、失機、觸霉頭': 292,
    '七A8白費勁、自找麻煩、沒辦法': 293,
    '七A9退縮、轉向、躲懶、過關': 294,
    '七A10給予、取要、挑選、使用、處置': 295,
    '七A11收存、收拾、遺失、尋找': 296,
    '七A12阻礙、佔據、分隔': 297,
    '七A13湊聚、併合、摻和': 298,
    '七A14排隊、插隊、圍攏、躲藏': 299,
    '七A15走動、離開、跟隨[走的動作參見六D11]': 300,
    '七A16節約、浪費、時興、過時': 301,
    '七A17稱、量、計算': 302,
    '七A18寫、塗': 303,
    '七A19笑、開玩笑、哭、歎息[笑、哭等的表情參見九A15]': 304,
    '七A20其他': 305,
    '七B1起臥、洗漱、穿著、脫衣': 306,
    '七B2烹調、購買食品': 307,
    '七B3飲食[飲食的動作參見六C2]': 308,
    '七B4帶孩子、刷洗縫補、室內事務[洗的動作參見六D9]': 309,
    '七B5生火、烤、熏、淬火': 310,
    '七B6上街、迷路、遷徙、旅行': 311,
    '七B7錢款進出': 312,
    '七B8遊戲、娛樂': 313,
    '七B9下棋、打牌': 314,
    '七B10戀愛、戀愛失敗': 315,
    '七B11婚嫁、其他喜事': 316,
    '七B12喪俗、舊俗、迷信活動、尋死': 317,
    '七B13其他': 318,
    '七C1說話、談話': 319,
    '七C2告訴、留話、吩咐、聽說': 320,
    '七C3不說話、支吾、私語': 321,
    '七C4能說會道、誇口、學舌': 322,
    '七C5稱讚、貶損、挖苦、戲弄': 323,
    '七C6斥責、爭吵、爭論、費口舌': 324,
    '七C7嘮叨、多嘴': 325,
    '七C8發牢騷、吵鬧、叫喊': 326,
    '七C9說粗話': 327,
    '七C10說謊、捏造': 328,
    '七C11其他': 329,
    '七D1工作、掙錢、辭退': 330,
    '七D2工業、建築、木工': 331,
    '七D3農副業': 332,
    '七D4交通、電訊': 333,
    '七D5商業': 334,
    '七D6服務行業[與商業相通的活動參見七D5]': 335,
    '七D7與商業、服務等行業有關的現象': 336,
    '七D8醫療': 337,
    '七D9教育、文化、新聞': 338,
    '七D10體育[棋、牌參見七B9]': 339,
    '七D11治安、執法': 340,
    '七D12其他': 341,
    '七E1相處、交好': 342,
    '七E2商量、邀約': 343,
    '七E3合作、拉線、散夥': 344,
    '七E4幫忙、施惠': 345,
    '七E5請求、督促、逼迫、支使': 346,
    '七E6守護、監視、査驗': 347,
    '七E7過問、聽任、容許、同意': 348,
    '七E8寵愛、遷就、撫慰': 349,
    '七E9討好、得罪、走門路': 350,
    '七E10炫耀、擺架子、謙遜、拜下風': 351,
    '七E11揭露、通消息': 352,
    '七E12責怪、激怒、翻臉': 353,
    '七E13做錯事、受責、丟臉': 354,
    '七E14爭鬥、較量': 355,
    '七E15打擊、揭短、嚇唬、驅趕': 356,
    '七E16欺負、霸道': 357,
    '七E17為難、捉弄、薄待': 358,
    '七E18出賣、使上當、陷害': 359,
    '七E19瞞騙、假裝、藉口': 360,
    '七E20拖累、妨害、胡鬧、搬弄是非': 361,
    '七E21蒙冤、上當、受氣、被迫': 362,
    '七E22發脾氣、撒野、撒嬌、耍賴': 363,
    '七E23打架、打人、勸架[與打相關的動作參見六D4]': 364,
    '七E24不良行為、犯罪活動': 365,
    '七E25禮貌用語、客套用語、祝願用語': 366,
    '七E26其他': 367,
    '八A1事情、案件、關係、原因': 368,
    '八A2形勢、境況、資訊': 369,
    '八A3嫌隙、冤仇、把柄': 370,
    '八A4命運、運氣、利益、福禍': 371,
    '八A5款式、條紋、形狀': 372,
    '八A6姿勢、舉動、相貌': 373,
    '八A7力量': 374,
    '八A8附：一般事物、這、那、甚麼[包括對各種事物的泛指和疑問，不一定是指抽象的事物]': 375,
    '八A9其他': 376,
    '八B1想法、脾氣、態度、品行': 377,
    '八B2本領、能力、技藝、素質': 378,
    '八B3計策、辦法、把握': 379,
    '八C1工作、行當、規矩、事務': 380,
    '八C2收入、費用、財產、錢款': 381,
    '八C3文化、娛樂、衛生': 382,
    '八C4情面、門路': 383,
    '八C5語言、文字': 384,
    '八C6其他': 385,
    '九A1大、小、粗、細': 386,
    '九A2長、短、高、矮、厚、薄': 387,
    '九A3寬、窄': 388,
    '九A4直、曲': 389,
    '九A5豎、斜、陡、正、歪': 390,
    '九A6尖利、禿鈍': 391,
    '九A7齊平、光滑、粗糙、凹凸、皺': 392,
    '九A8胖、壯、臃腫、瘦': 393,
    '九A9其他形狀[另參見八A5]': 394,
    '九A10亮、清晰、暗、模糊': 395,
    '九A11顏色': 396,
    '九A12鮮艷、奪目、樸素、暗淡': 397,
    '九A13美、精緻、難看': 398,
    '九A14新、舊': 399,
    '九A15表情、臉色、相貌[哭、笑的動作參見七A19；相貌另參見二C11、15及八A6]': 400,
    '九B1冷、涼、暖、熱、燙': 401,
    '九B2乾燥、潮濕、多水': 402,
    '九B3稠、濃、黏、稀': 403,
    '九B4硬、結實、軟、韌、脆': 404,
    '九B5空、通、漏、堵塞、封閉': 405,
    '九B6密、滿、擠、緊、疏、鬆': 406,
    '九B7整齊、均勻、吻合、亂、不相配': 407,
    '九B8穩定、不穩、顛簸[搖動參見六A4]': 408,
    '九B9零碎、潦草、骯髒': 409,
    '九B10破損、破爛、脫落': 410,
    '九B11腐爛、發黴、褪色': 411,
    '九B12壓、硌、絆、卡、礙、累贅': 412,
    '九B13顛倒、反扣': 413,
    '九B14相連、糾結、吊、垂': 414,
    '九B15裸露、遮蓋[遮蓋的動作參見六D7]': 415,
    '九B16淹、沉、浮、洇、凝': 416,
    '九B17遠、近': 417,
    '九B18多、少': 418,
    '九B19重、輕': 419,
    '九B20氣味': 420,
    '九B21味道': 421,
    '九B22可口、難吃、味濃、味淡': 422,
    '九B23其他': 423,
    '九C1妥當、順利、得志、吉利、好運': 424,
    '九C2不利、失敗、嚴峻、緊急': 425,
    '九C3倒楣、糟糕、運氣差': 426,
    '九C4狼狽、有麻煩、淒慘、可憐': 427,
    '九C5舒服、富有、辛苦、貧窮': 428,
    '九C6洋氣、派頭、土氣、寒磣': 429,
    '九C7繁忙、空閒': 430,
    '九C8手快、匆忙、緊張、手慢、悠遊': 431,
    '九C9熟悉、生疏、坦率、露骨': 432,
    '九C10和睦、合得來、不和、合不來': 433,
    '九C11合算、不合算': 434,
    '九C12有條理、沒條理、馬虎、隨便、離譜': 435,
    '九C13兇狠、可厭': 436,
    '九C14其他': 437,
    '九D1好、水準高、比得上': 438,
    '九D2不好、水準低、中等、差得遠': 439,
    '九D3真實、虛假、正確、謬誤': 440,
    '九D4頂用、好用、無用、禁得起、禁不起': 441,
    '九D5變化、不變、有關、無關': 442,
    '九D6增加、減少、沒有、不充足、欠缺': 443,
    '九D7能夠、不行、有望、無望、有收益、無收益': 444,
    '九D8到時、過點、來得及、來不及': 445,
    '九D9困難、危險、可怕、容易、淺顯': 446,
    '九D10奇怪、無端、難怪': 447,
    '九D11有趣、滑稽、枯燥': 448,
    '九D12嘈雜、聲音大、靜、聲音小': 449,
    '九D13熱鬧、排場、冷清、偏僻': 450,
    '九D14早、遲、久、暫、快、慢': 451,
    '九D15厲害、很、過分、最、更、甚至': 452,
    '九D16稍微、有點、差不多、幾乎': 453,
    '九D17經常、不斷、總是、長期、一向、動輒': 454,
    '九D18不時、間或、偶然、又、再、重新': 455,
    '九D19極度、勉強、儘量、直接': 456,
    '九D20肯定、也還、應該、千萬': 457,
    '九D21全部、一同、也都、獨自、雙方': 458,
    '九D22正在、起來、下去、已經、曾經': 459,
    '九D23剛剛、待會兒、將要、立刻、突然、碰巧': 460,
    '九D24終於、預先、臨時、暫且、再說、幸好': 461,
    '九D25和、或者、要麼、不然、衹好': 462,
    '九D26然後、接著、才、於是、至於': 463,
    '九D27不但、而且、且不說': 464,
    '九D28因為、所以、既然、反正、為了、免得': 465,
    '九D29如果、無論、那麼、除了': 466,
    '九D30固然、但是、不過、反而、還': 467,
    '九D31是、像、可能、原本、實際上、在': 468,
    '九D32不、不是、不要、不必、不曾': 469,
    '九D33這樣、那樣、怎樣、為甚麼、難道': 470,
    '九D34其他': 471,
    '十A1數目': 472,
    '十A2概數、成數': 473,
    '十B1人的計量單位[一般用"個"，與普通話一樣]': 474,
    '十B2動植物的計量單位': 475,
    '十B3人體部位等的計量單位': 476,
    '十C1不同組成的物體的量（個、雙、套等）': 477,
    '十C2不同形狀的物體的量（塊、條、片等）': 478,
    '十C3不同排列的物體的量（串、排、把等）': 479,
    '十C4分類的物品的量': 480,
    '十C5食物的量': 481,
    '十C6某些特定用品、物品的量': 482,
    '十C7其他': 483,
    '十D1貨幣單位': 484,
    '十D2度量衡單位': 485,
    '十E1時間的量': 486,
    '十E2空間、長度的量': 487,
    '十F1抽象事物的量': 488,
    '十F2動作的量': 489,
    '十一A1用在句末表示敘述、肯定等的語氣詞': 490,
    '十一A2用在句末表示問話的語氣詞': 491,
    '十一A3單獨使用的表語氣詞語（嘆詞）': 492,
    '十一B1自然界的聲音': 493,
    '十一B2人發出的聲音': 494,
    '十一B3人造成的聲音': 495,
    '十一C1口頭禪、慣用語': 496,
    '十一C2歇後語': 497,
    '十一C3諺語': 498,
}

def clean_text(text):
    """清理文本，转义SQL特殊字符"""
    if not text:
        return ''
    
    # 替换单引号为两个单引号进行转义
    text = text.replace("'", "''")
    # 去除可能导致问题的特殊字符
    text = text.replace('\x00', '')
    return text

def normalize_text(text):
    """标准化文本用于搜索"""
    if not text:
        return ''
    
    # 去除特殊标记符号
    text = re.sub(r'[*\(\)\[\]【】]', '', text)
    # 去除多余空格
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def determine_region(text, jyutping, note):
    """根据文本和注释确定地区"""
    # 本次数据全部来自广州地区
    return 'guangzhou'

def determine_formality(text, meanings, note):
    """根据文本和注释确定正式程度"""
    if not text or not meanings:
        return 'neutral'
    
    note_text = (note or '')
    meanings_text = meanings
    combined_text = meanings_text + note_text
    
    # 根据标记判断正式程度
    # 粗俗/侮辱性语言
    if '【罵】' in combined_text or '【侮】' in combined_text:
        return 'vulgar'
    
    # 俚语/俗语（包括粗话）
    elif '【俗】' in combined_text:
        return 'slang'
    
    # 正式/文雅用语
    elif '【雅】' in combined_text or '【敬】' in combined_text or '【婉】' in combined_text:
        return 'formal'
    
    # 非正式用语（戏语、亲昵称呼、儿语等）
    elif '【戲】' in combined_text or '【親】' in combined_text or '【兒】' in combined_text:
        return 'informal'
    
    # 其他情况默认为中性（包括【褒】【貶】【喻】【舊】【外】【熟】【歇】等）
    else:
        return 'neutral'

def determine_frequency(text):
    """根据文本特征确定使用频率，无法确定则返回NULL"""
    return None

def get_theme_ids(category_3, category_2, category_1):
    """获取三级主题ID"""
    theme_l1 = None
    theme_l2 = None  
    theme_l3 = None
    
    # 获取一级主题ID
    if category_1 and category_1 in THEME_MAPPING:
        theme_l1 = THEME_MAPPING[category_1]
    
    # 获取二级主题ID
    if category_2 and category_2 in THEME_MAPPING:
        theme_l2 = THEME_MAPPING[category_2]
    
    # 获取三级主题ID
    if category_3 and category_3 in THEME_MAPPING:
        theme_l3 = THEME_MAPPING[category_3]
    
    return theme_l1, theme_l2, theme_l3

def generate_sql():
    """生成SQL插入语句"""
    expressions_sql = []
    
    # 默认用户ID (需要在实际部署时替换为真实的用户UUID)
    default_user_id = '\'9056ca72-cdaf-4288-99b6-0c2d6eb298c9\''
    
    print("开始处理CSV文件...")
    
    with open('Satgwong_processed.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row_num, row in enumerate(reader, 1):
            if row_num % 1000 == 0:
                print(f"已处理 {row_num} 条记录...")
            
            # 提取数据
            words = row.get('words', '').strip()
            jyutping = row.get('jyutping', '').strip()
            meanings = row.get('meanings', '').strip()
            note = row.get('note', '').strip()
            category_1 = row.get('category_1', '').strip()
            category_2 = row.get('category_2', '').strip()
            category_3 = row.get('category_3', '').strip()
            
            if not words:
                continue
            
            # 获取三级主题ID
            theme_l1, theme_l2, theme_l3 = get_theme_ids(category_3, category_2, category_1)
            
            # 清理和处理文本
            clean_words = clean_text(words)
            clean_meanings = clean_text(meanings)
            clean_note = clean_text(note)
            clean_jyutping = clean_text(jyutping) if jyutping else ''
            
            # 标准化文本
            normalized_text = normalize_text(words)
            
            # 确定属性
            region = determine_region(words, jyutping, note)
            formality = determine_formality(words, meanings, note)
            frequency = determine_frequency(words)
            
            # 处理NULL值的情况
            theme_l1_value = theme_l1 if theme_l1 is not None else 'NULL'
            theme_l2_value = theme_l2 if theme_l2 is not None else 'NULL'
            theme_l3_value = theme_l3 if theme_l3 is not None else 'NULL'
            frequency_value = 'NULL' if frequency is None else f"'{frequency}'"
            phonetic_value = f"'{clean_jyutping}'" if clean_jyutping else 'NULL'
            
            # 生成expressions表的INSERT语句
            expression_sql = f"""INSERT INTO expressions (
  theme_id_l1, theme_id_l2, theme_id_l3, text, text_normalized, region, 
  definition, usage_notes, formality_level, frequency, 
  phonetic_notation, notation_system, pronunciation_verified,
  contributor_id, status, created_at, updated_at
) VALUES (
  {theme_l1_value}, {theme_l2_value}, {theme_l3_value}, '{clean_words}', '{normalize_text(clean_words)}', '{region}', 
  '{clean_meanings}', '{clean_note}', '{formality}', {frequency_value}, 
  {phonetic_value}, 'jyutping++', {str(jyutping != '').lower()},
  {default_user_id}, 'approved', NOW(), NOW()
);"""
            
            expressions_sql.append(expression_sql)
    
    print(f"处理完成！共生成 {len(expressions_sql)} 条expressions记录")
    
    # 分割写入SQL文件 (每1000行一个文件)
    max_lines_per_file = 1000
    file_count = 0
    current_line_count = 0
    current_file = None
    
    try:
        for i, sql in enumerate(expressions_sql):
            # 检查是否需要创建新文件
            if current_file is None or current_line_count >= max_lines_per_file:
                # 关闭前一个文件
                if current_file:
                    current_file.write("\n-- 提交事务\n")
                    current_file.write("COMMIT;\n")
                    current_file.close()
                    print(f"SQL文件已生成：expressions_insert_part{file_count:03d}.sql ({current_line_count} 条记录)")
                
                # 创建新文件
                file_count += 1
                current_line_count = 0
                filename = f'expressions_insert_part{file_count:03d}.sql'
                current_file = open(filename, 'w', encoding='utf-8')
                
                # 写入文件头部
                current_file.write("-- 自动生成的expressions表INSERT语句\n")
                current_file.write(f"-- 基于Satgwong_processed.csv文件 (第{file_count}部分)\n\n")
                current_file.write("-- 开始事务\n")
                current_file.write("BEGIN;\n\n")
                
                # 只在第一个文件中写入删除语句
                if file_count == 1:
                    current_file.write("-- 清空现有数据（可选，谨慎使用）\n")
                    current_file.write("-- DELETE FROM expressions;\n\n")
                
                current_file.write("-- 插入expressions数据\n")
            
            # 写入SQL语句
            current_file.write(sql + "\n")
            current_line_count += 1
        
        # 关闭最后一个文件
        if current_file:
            current_file.write("\n-- 提交事务\n")
            current_file.write("COMMIT;\n")
            current_file.close()
            print(f"SQL文件已生成：expressions_insert_part{file_count:03d}.sql ({current_line_count} 条记录)")
        
        print(f"总共生成了 {file_count} 个SQL文件")
        
    except Exception as e:
        if current_file:
            current_file.close()
        raise e
    
    # 生成统计信息
    with open('expressions_analysis_report.txt', 'w', encoding='utf-8') as f:
        f.write("Satgwong数据库插入统计\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"总记录数: {len(expressions_sql)}\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # 按分类统计
        category_stats = {}
        with open('Satgwong_processed.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                cat1 = row.get('category_1', '').strip()
                if cat1:
                    category_stats[cat1] = category_stats.get(cat1, 0) + 1
        
        f.write("按一级分类统计:\n")
        for cat, count in sorted(category_stats.items()):
            f.write(f"  {cat}: {count} 条\n")
    
    print("统计文件已生成：expressions_analysis_report.txt")

if __name__ == '__main__':
    generate_sql() 