"""
SQLAlchemy models mirroring key Salesforce objects.
"""
from sqlalchemy import Column, String, Float, Integer, Boolean, Date, DateTime, Text, Index
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime


class Base(DeclarativeBase):
    pass


class Student(Base):
    __tablename__ = "students"

    sf_id = Column(String(18), primary_key=True)
    name = Column(String(255), index=True)
    student_marketing_status = Column(String(100), index=True)
    technology = Column(String(255), index=True)
    manager_id = Column(String(18), index=True)
    manager_name = Column(String(255), index=True)
    phone = Column(String(50))
    email = Column(String(255))
    marketing_visa_status = Column(String(100), index=True)
    days_in_market = Column(Float)
    last_submission_date = Column(Date)
    pre_marketing_status = Column(String(100))
    verbal_confirmation_date = Column(Date)
    project_start_date = Column(Date)
    resume_preparation = Column(String(100))
    resume_verified_by_lead = Column(String(100))
    resume_verified_by_manager = Column(String(100))
    resume_verification = Column(String(100))
    resume_review = Column(String(100))
    otter_screening = Column(String(100))
    otter_final_screening = Column(String(100))
    otter_real_time_1 = Column(String(100))
    otter_real_time_2 = Column(String(100))
    has_linkedin_created = Column(String(100))
    student_linkedin_review = Column(String(100))
    mq_screening_by_lead = Column(String(100))
    mq_screening_by_manager = Column(String(100))
    offshore_manager_name = Column(String(255))
    synced_at = Column(DateTime, default=datetime.utcnow)


class Submission(Base):
    __tablename__ = "submissions"

    sf_id = Column(String(18), primary_key=True)
    student_name = Column(String(255), index=True)
    bu_name = Column(String(255), index=True)
    client_name = Column(String(255))
    submission_date = Column(Date, index=True)
    offshore_manager_name = Column(String(255), index=True)
    recruiter_name = Column(String(255), index=True)
    created_date = Column(DateTime, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow)


class Interview(Base):
    __tablename__ = "interviews"

    sf_id = Column(String(18), primary_key=True)
    student_id = Column(String(18), index=True)
    student_name = Column(String(255), index=True)
    onsite_manager = Column(String(255), index=True)
    offshore_manager = Column(String(255))
    interview_type = Column(String(100))
    final_status = Column(String(100), index=True)
    amount = Column(Float)
    amount_inr = Column(Float)
    bill_rate = Column(Float)
    interview_date = Column(Date, index=True)
    created_date = Column(DateTime, index=True)
    synced_at = Column(DateTime, default=datetime.utcnow)


class Manager(Base):
    __tablename__ = "managers"

    sf_id = Column(String(18), primary_key=True)
    name = Column(String(255), index=True)
    active = Column(Boolean, default=True)
    total_expenses = Column(Float)
    each_placement_cost = Column(Float)
    students_count = Column(Float)
    in_market_students_count = Column(Float)
    verbal_count = Column(Float)
    bu_student_with_job_count = Column(Float)
    in_job_students_count = Column(Float)
    cluster = Column(String(255))
    organization = Column(String(255))
    synced_at = Column(DateTime, default=datetime.utcnow)


class Job(Base):
    __tablename__ = "jobs"

    sf_id = Column(String(18), primary_key=True)
    student_id = Column(String(18), index=True)
    student_name = Column(String(255), index=True)
    share_with_id = Column(String(18))
    share_with_name = Column(String(255), index=True)
    pay_rate = Column(Float)
    calculated_pay_rate = Column(Float)
    pay_roll_tax = Column(Float)
    profit = Column(Float)
    bill_rate = Column(Float)
    active = Column(Boolean, index=True)
    project_type = Column(String(100))
    technology = Column(String(255))
    payroll_month = Column(String(50))
    synced_at = Column(DateTime, default=datetime.utcnow)


class Employee(Base):
    __tablename__ = "employees"

    sf_id = Column(String(18), primary_key=True)
    name = Column(String(255), index=True)
    onshore_manager_id = Column(String(18))
    onshore_manager_name = Column(String(255))
    cluster = Column(String(255))
    synced_at = Column(DateTime, default=datetime.utcnow)


class User(Base):
    __tablename__ = "users"

    username = Column(String(100), primary_key=True)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(255))
    role = Column(String(20), default="user")
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id = Column(String(100), primary_key=True)
    username = Column(String(100), index=True, nullable=False)
    title = Column(String(255), default="New Chat")
    pinned = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id = Column(String(100), primary_key=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)
    content = Column(Text)
    soql = Column(Text)
    data = Column(Text)
    ts = Column(DateTime, default=datetime.utcnow)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    username = Column(String(100), index=True)
    action = Column(String(100), index=True)
    details = Column(Text)
    ip_address = Column(String(50))


class SyncLog(Base):
    __tablename__ = "sync_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    object_name = Column(String(100), index=True)
    records_synced = Column(Integer)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    status = Column(String(20))
    error = Column(Text)
