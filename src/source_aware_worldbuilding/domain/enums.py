from enum import StrEnum


class ClaimStatus(StrEnum):
    VERIFIED = "verified"
    PROBABLE = "probable"
    CONTESTED = "contested"
    RUMOR = "rumor"
    LEGEND = "legend"
    AUTHOR_CHOICE = "author_choice"


class ReviewState(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    NEEDS_SPLIT = "needs_split"
    NEEDS_EDIT = "needs_edit"
    SUPERSEDED = "superseded"


class ClaimKind(StrEnum):
    PERSON = "person"
    PLACE = "place"
    INSTITUTION = "institution"
    EVENT = "event"
    PRACTICE = "practice"
    BELIEF = "belief"
    RELATIONSHIP = "relationship"
    OBJECT = "object"


class QueryMode(StrEnum):
    STRICT_FACTS = "strict_facts"
    CONTESTED_VIEWS = "contested_views"
    RUMOR_AND_LEGEND = "rumor_and_legend"
    CHARACTER_KNOWLEDGE = "character_knowledge"
    OPEN_EXPLORATION = "open_exploration"


class ReviewDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class ExtractionRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
