"""
地方文书管理系统 - 数据模型与常用数据库操作

说明：
- 使用 SQLAlchemy 连接 MySQL，并通过 ORM 定义核心数据表模型：
  User（用户）、Document（文书）、Resource（资源）、Note（笔记）、Favorite（收藏）
- 提供若干函数完成实验/课程要求的“数据库基本功能 + 高级功能”示例。

在使用前，请先确保已经在 MySQL 中执行过 `db.sql` 中的建库和建表语句。
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Tuple

from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    create_engine,
    func,
    text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship, Session, sessionmaker
from flask_login import UserMixin

# ============================================================
# 1. 数据库连接配置
# ============================================================
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "M17382930994c@" #这里改为自己的密码
DB_NAME = "our_document"

DATABASE_URL = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?charset=utf8mb4"
)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_session() -> Session:
    """获取数据库会话（调用方需手动关闭）"""
    return SessionLocal()


# ============================================================
# 2. ORM 模型定义（严格对齐 db.sql）
# ============================================================
class User(Base, UserMixin):
    """用户信息表 - user_info"""
    __tablename__ = "user_info"

    user_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='用户ID')
    username = Column(String(30), unique=True, nullable=False, comment='用户名')
    user_email = Column(String(30), unique=True, nullable=False, comment='用户邮箱')
    password = Column(String(255), nullable=False, comment='密码')
    pr_question = Column(Text, comment='找回密码问题')
    pr_answer = Column(Text, comment='找回密码答案')

    # 关联关系
    notes = relationship("Note", back_populates="user")
    favorites = relationship("Favorite", back_populates="user")
    access_records = relationship("AccessRecord", back_populates="user")


class Document(Base):
    """文书信息表 - document_info"""
    __tablename__ = "document_info"

    document_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='文书ID')
    document_name = Column(String(255), unique=True, nullable=False, comment='文书名称')
    document_region = Column(String(255), comment='文书区域')
    document_intro = Column(Text, comment='文书简介')
    document_cover = Column(Text, comment='文书封面（二进制）')

    # 关联关系
    resources = relationship("Resource", back_populates="document")


class Resource(Base):
    """资源内容表 - resource_content"""
    __tablename__ = "resource_content"

    resource_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='资源ID')
    document_id = Column(BigInteger, ForeignKey("document_info.document_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='文书ID')
    resource_name = Column(String(255), unique=True, nullable=False, comment='资源名称')
    resource_type = Column(String(255), comment='资源类型')
    original_text = Column(Text, comment='原文')
    simplified_text = Column(Text, comment='简体版全文')
    vernacular_translation = Column(Text, comment='白话文翻译')

    # 关联关系
    document = relationship("Document", back_populates="resources")
    info = relationship("ResourceInfo", back_populates="resource", uselist=False)
    images = relationship("ResourceImage", back_populates="resource")
    carrier = relationship("ResourceCarrier", back_populates="resource", uselist=False)
    notes = relationship("Note", back_populates="resource")
    favorites = relationship("Favorite", back_populates="resource")
    access_records = relationship("AccessRecord", back_populates="resource")


class ResourceInfo(Base):
    """资源信息表 - resource_info"""
    __tablename__ = "resource_info"

    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True, comment='资源ID')
    dynasty_period = Column(String(255), comment='年代')
    reign_title = Column(String(255), comment='年号')
    resource_region = Column(String(255), comment='资源区域')
    household_registry = Column(String(255), comment='归户')
    author = Column(String(100), comment='作者')

    # 关联关系
    resource = relationship("Resource", back_populates="info")


class ResourceImage(Base):
    """资源图片表 - resource_image"""
    __tablename__ = "resource_image"

    image_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='图片ID')
    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='资源ID')
    page = Column(BigInteger, comment='页码')

    # 关联关系
    resource = relationship("Resource", back_populates="images")


class ResourceCarrier(Base):
    """资源载体表 - resource_carrier"""
    __tablename__ = "resource_carrier"

    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), primary_key=True, comment='资源ID')
    resource_material = Column(String(255), comment='文献材质')
    resource_dimensions = Column(String(255), comment='文献尺寸')

    # 关联关系
    resource = relationship("Resource", back_populates="carrier")


class Favorite(Base):
    """收藏信息表 - collection_info"""
    __tablename__ = "collection_info"

    # 唯一约束（对齐db.sql）
    __table_args__ = (
        UniqueConstraint('user_id', 'resource_id', name='uk_user_resource'),
    )

    collection_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='收藏ID')
    user_id = Column(BigInteger, ForeignKey("user_info.user_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='用户ID')
    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='资源ID')
    collection_tags = Column(String(20), comment='收藏标签')
    collection_time = Column(DateTime, default=datetime.utcnow, nullable=False, comment='收藏时间')

    # 关联关系
    user = relationship("User", back_populates="favorites")
    resource = relationship("Resource", back_populates="favorites")

    @property
    def document(self):
        return self.resource.document if self.resource else None


class Note(Base):
    """笔记表 - annotation"""
    __tablename__ = "annotation"

    annotation_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='笔记ID')
    user_id = Column(BigInteger, ForeignKey("user_info.user_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='用户ID')
    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='资源ID')
    annotation_content = Column(Text, comment='笔记内容')
    annotation_tags = Column(String(20), comment='笔记标签')
    annotation_time = Column(DateTime, default=datetime.utcnow, nullable=False, comment='创建时间')
    update_time = Column(DateTime, default=datetime.utcnow, nullable=False, comment='更新时间')

    # 关联关系
    user = relationship("User", back_populates="notes")
    resource = relationship("Resource", back_populates="resource")

    @property
    def document(self):
        return self.resource.document if self.resource else None


class AccessRecord(Base):
    """阅读记录表 - access_record（替换原ReadRecord）"""
    __tablename__ = "access_record"

    # 唯一约束（对齐db.sql）
    __table_args__ = (
        UniqueConstraint('user_id', 'resource_id', name='uk_user_resource'),
    )

    access_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='阅读记录ID')
    user_id = Column(BigInteger, ForeignKey("user_info.user_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='用户ID')
    resource_id = Column(BigInteger, ForeignKey("resource_content.resource_id", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment='资源ID')
    access_time = Column(DateTime, default=datetime.utcnow, nullable=False, comment='访问时间')
    read_progress = Column(Integer, default=0, comment='阅读进度（扩展字段）')

    # 关联关系
    user = relationship("User", back_populates="access_records")
    resource = relationship("Resource", back_populates="access_records")

    @property
    def document(self):
        return self.resource.document if self.resource else None


# 视图模型（只读，对齐db.sql的视图）
class DocumentStats(Base):
    """文书统计视图 - v_document_resource_stats"""
    __tablename__ = "v_document_resource_stats"
    __table_args__ = {"info": {"is_view": True}}

    document_id = Column(BigInteger, primary_key=True)
    document_name = Column(String(255))
    document_region = Column(String(255))
    resource_count = Column(Integer)
    image_count = Column(Integer)
    collection_count = Column(Integer)
    annotation_count = Column(Integer)


# ============================================================
# 3. 基础查询功能（对齐db.sql的业务需求）
# ============================================================
def get_resources_by_document(session: Session, document_id: int) -> List[Resource]:
    """查询某文书的所有资源"""
    return (
        session.query(Resource)
        .filter(Resource.document_id == document_id)
        .order_by(Resource.resource_id)
        .all()
    )


def get_documents_by_author(session: Session, author: str) -> List[Document]:
    """查询特定作者的所有文书（关联resource_info）"""
    sql = text("""
        SELECT DISTINCT d.*
        FROM document_info d
        JOIN resource_content rc ON d.document_id = rc.document_id
        JOIN resource_info ri ON rc.resource_id = ri.resource_id
        WHERE ri.author = :author
    """)
    result = session.execute(sql, {"author": author})
    document_ids = [row.document_id for row in result]
    if not document_ids:
        return []
    return (
        session.query(Document)
        .filter(Document.document_id.in_(document_ids))
        .order_by(Document.document_id)
        .all()
    )


def search_transcription_by_keyword(session: Session, keyword: str) -> List[Resource]:
    """模糊查询包含关键词的转录文本"""
    pattern = f"%{keyword}%"
    return (
        session.query(Resource)
        .filter(
            (Resource.simplified_text.ilike(pattern))
            | (Resource.vernacular_translation.ilike(pattern))
        )
        .all()
    )


def get_favorite_documents_by_user(session: Session, user_id: int) -> List[Document]:
    """查询用户收藏的文书"""
    return (
        session.query(Document)
        .join(Resource, Resource.document_id == Document.document_id)
        .join(Favorite, Favorite.resource_id == Resource.resource_id)
        .filter(Favorite.user_id == user_id)
        .distinct()
        .order_by(Document.document_id)
        .all()
    )


def count_resources_by_document(session: Session, document_id: int) -> int:
    """统计文书的资源数量"""
    return (
        session.query(func.count(Resource.resource_id))
        .filter(Resource.document_id == document_id)
        .scalar() or 0
    )


def count_resources_by_document_and_type(session: Session, document_id: int, resource_type: str) -> int:
    """统计文书中指定类型的资源数量"""
    return (
        session.query(func.count(Resource.resource_id))
        .filter(
            Resource.document_id == document_id,
            Resource.resource_type == resource_type
        )
        .scalar() or 0
    )


# ============================================================
# 4. 高级功能：存储过程/触发器/全文索引（对齐db.sql）
# ============================================================
def init_advanced_db_features(session: Session) -> None:
    """初始化数据库高级特性（幂等设计）"""
    # 1. 存储过程：插入文书
    session.execute(text("DROP PROCEDURE IF EXISTS sp_insert_document;"))
    session.execute(text("""
        CREATE PROCEDURE sp_insert_document(
            IN p_name VARCHAR(255),
            IN p_region VARCHAR(255),
            IN p_intro TEXT
        )
        BEGIN
            INSERT INTO document_info(document_name, document_region, document_intro)
            VALUES(p_name, p_region, p_intro);
            SELECT LAST_INSERT_ID() AS document_id;
        END;
    """))

    # 2. 触发器：维护笔记时间字段
    session.execute(text("DROP TRIGGER IF EXISTS trg_annotation_before_insert;"))
    session.execute(text("DROP TRIGGER IF EXISTS trg_annotation_before_update;"))
    session.execute(text("""
        CREATE TRIGGER trg_annotation_before_insert
        BEFORE INSERT ON annotation
        FOR EACH ROW
        BEGIN
            IF NEW.annotation_time IS NULL THEN
                SET NEW.annotation_time = NOW();
            END IF;
            SET NEW.update_time = NEW.annotation_time;
        END;
    """))
    session.execute(text("""
        CREATE TRIGGER trg_annotation_before_update
        BEFORE UPDATE ON annotation
        FOR EACH ROW
        BEGIN
            SET NEW.update_time = NOW();
        END;
    """))

    # 3. 全文索引（对齐db.sql的注释）
    session.execute(text("""
        ALTER TABLE resource_content ADD FULLTEXT INDEX IF NOT EXISTS ft_original_text (original_text);
        ALTER TABLE resource_content ADD FULLTEXT INDEX IF NOT EXISTS ft_simplified_text (simplified_text);
        ALTER TABLE resource_content ADD FULLTEXT INDEX IF NOT EXISTS ft_vernacular_translation (vernacular_translation);
        ALTER TABLE document_info ADD FULLTEXT INDEX IF NOT EXISTS ft_document_intro (document_intro);
        ALTER TABLE annotation ADD FULLTEXT INDEX IF NOT EXISTS ft_annotation_content (annotation_content);
    """))

    # 4. 补充阅读进度字段（扩展db.sql）
    session.execute(text("""
        ALTER TABLE access_record 
        ADD COLUMN IF NOT EXISTS read_progress INT DEFAULT 0 COMMENT '阅读进度' AFTER access_time;
    """))

    session.commit()


def call_insert_document_procedure(session: Session, name: str, region: Optional[str], intro: Optional[str]) -> int:
    """调用存储过程插入文书"""
    result = session.execute(text("CALL sp_insert_document(:name, :region, :intro);"),
                             {"name": name, "region": region, "intro": intro})
    row = result.fetchone()
    return int(row.document_id) if row and hasattr(row, "document_id") else 0


def fulltext_search_resources(session: Session, keyword: str) -> List[Resource]:
    """全文检索资源文本"""
    sql = text("""
        SELECT *
        FROM resource_content
        WHERE MATCH(original_text, simplified_text, vernacular_translation)
              AGAINST (:kw IN NATURAL LANGUAGE MODE)
    """)
    result = session.execute(sql, {"kw": keyword})
    resource_ids = [row.resource_id for row in result]
    if not resource_ids:
        return []
    return (
        session.query(Resource)
        .filter(Resource.resource_id.in_(resource_ids))
        .all()
    )


def get_document_stats(session: Session) -> List[DocumentStats]:
    """查询文书统计视图"""
    return session.query(DocumentStats).order_by(DocumentStats.document_id).all()


# ============================================================
# 5. 文书CRUD
# ============================================================
def create_document(session: Session, name: str, region: Optional[str] = None, intro: Optional[str] = None) -> Document:
    """创建文书"""
    doc = Document(document_name=name, document_region=region, document_intro=intro)
    session.add(doc)
    session.commit()
    session.refresh(doc)
    return doc


def get_document_by_id(session: Session, document_id: int) -> Optional[Document]:
    """按ID查询文书"""
    return session.get(Document, document_id)


def update_document(session: Session, document_id: int, name: Optional[str] = None, region: Optional[str] = None, intro: Optional[str] = None) -> Optional[Document]:
    """更新文书"""
    doc = session.get(Document, document_id)
    if not doc:
        return None
    if name is not None:
        doc.document_name = name
    if region is not None:
        doc.document_region = region
    if intro is not None:
        doc.document_intro = intro
    session.commit()
    session.refresh(doc)
    return doc


def delete_document(session: Session, document_id: int) -> bool:
    """删除文书"""
    doc = session.get(Document, document_id)
    if not doc:
        return False
    session.delete(doc)
    session.commit()
    return True


# ============================================================
# 6. 笔记操作函数
# ============================================================
def create_note(session: Session, user_id: int, resource_id: int, content: str, tags: Optional[str] = None) -> Note:
    """创建笔记"""
    note = Note(
        user_id=user_id,
        resource_id=resource_id,
        annotation_content=content,
        annotation_tags=tags
    )
    session.add(note)
    session.commit()
    session.refresh(note)
    return note


def get_notes_by_user(session: Session, user_id: int) -> List[Note]:
    """查询用户所有笔记"""
    return (
        session.query(Note)
        .filter(Note.user_id == user_id)
        .order_by(Note.update_time.desc())
        .all()
    )


def get_notes_by_resource(session: Session, resource_id: int) -> List[Note]:
    """查询资源的所有笔记"""
    return (
        session.query(Note)
        .filter(Note.resource_id == resource_id)
        .order_by(Note.annotation_time.desc())
        .all()
    )


def update_note(session: Session, note_id: int, content: Optional[str] = None, tags: Optional[str] = None) -> Optional[Note]:
    """更新笔记"""
    note = session.get(Note, note_id)
    if not note:
        return None
    if content is not None:
        note.annotation_content = content
    if tags is not None:
        note.annotation_tags = tags
    session.commit()
    session.refresh(note)
    return note


def delete_note(session: Session, note_id: int) -> bool:
    """删除笔记"""
    note = session.get(Note, note_id)
    if not note:
        return False
    session.delete(note)
    session.commit()
    return True


# ============================================================
# 7. 收藏操作函数
# ============================================================
def toggle_favorite(session: Session, user_id: int, resource_id: int, tags: Optional[str] = None) -> Tuple[bool, bool]:
    """切换收藏状态（有则取消，无则添加）"""
    favorite = session.query(Favorite).filter(
        Favorite.user_id == user_id,
        Favorite.resource_id == resource_id
    ).first()

    if favorite:
        # 取消收藏
        session.delete(favorite)
        session.commit()
        return True, False
    else:
        # 添加收藏
        new_fav = Favorite(
            user_id=user_id,
            resource_id=resource_id,
            collection_tags=tags
        )
        session.add(new_fav)
        session.commit()
        return True, True


def is_resource_favorited(session: Session, user_id: int, resource_id: int) -> bool:
    """判断资源是否被用户收藏"""
    return session.query(Favorite).filter(
        Favorite.user_id == user_id,
        Favorite.resource_id == resource_id
    ).first() is not None


# ============================================================
# 8. 阅读记录操作函数
# ============================================================
def create_access_record(session: Session, user_id: int, resource_id: int, progress: int = 0) -> AccessRecord:
    """创建/更新阅读记录（UPSERT）"""
    record = session.query(AccessRecord).filter(
        AccessRecord.user_id == user_id,
        AccessRecord.resource_id == resource_id
    ).first()

    if record:
        # 更新现有记录
        record.access_time = datetime.utcnow()
        record.read_progress = progress
    else:
        # 新建记录
        record = AccessRecord(
            user_id=user_id,
            resource_id=resource_id,
            read_progress=progress
        )
        session.add(record)

    session.commit()
    session.refresh(record)
    return record


def get_access_records_by_user(session: Session, user_id: int) -> List[AccessRecord]:
    """查询用户所有阅读记录"""
    return (
        session.query(AccessRecord)
        .filter(AccessRecord.user_id == user_id)
        .order_by(AccessRecord.access_time.desc())
        .all()
    )


# ============================================================
# 9. 测试入口
# ============================================================
if __name__ == "__main__":
    with get_session() as s:
        try:
            init_advanced_db_features(s)
            print("数据库高级特性初始化成功！")
            
            # 打印文书统计
            stats = get_document_stats(s)
            print(f"当前文书数量：{len(stats)}")
            for stat in stats:
                print(f"文书ID: {stat.document_id}, 名称: {stat.document_name}, 资源数: {stat.resource_count}")
        except Exception as e:
            print(f"初始化失败：{str(e)}")