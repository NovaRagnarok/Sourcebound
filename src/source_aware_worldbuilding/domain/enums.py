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


class BibleSectionType(StrEnum):
    SETTING_OVERVIEW = "setting_overview"
    CHRONOLOGY = "chronology"
    PEOPLE_AND_FACTIONS = "people_and_factions"
    DAILY_LIFE = "daily_life"
    INSTITUTIONS_AND_POLITICS = "institutions_and_politics"
    ECONOMICS_AND_MATERIAL_CULTURE = "economics_and_material_culture"
    RUMORS_AND_CONTESTED = "rumors_and_contested_accounts"
    AUTHOR_DECISIONS = "author_decisions"


class BibleTone(StrEnum):
    DOCUMENTARY = "documentary"
    GROUNDED_LITERARY = "grounded_literary"
    RUMOR_RICH = "rumor_rich_folkloric"
    MIXED = "mixed_historical_fiction"


class ReviewDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class ExtractionRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ResearchRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPLETED_PARTIAL = "completed_partial"
    FAILED_POLICY = "failed_policy"
    FAILED_RUNTIME = "failed_runtime"
    CANCELLED = "cancelled"
    DEGRADED_FALLBACK = "degraded_fallback"
    FAILED = "failed"


class ResearchFindingDecision(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class ResearchFindingReason(StrEnum):
    ACCEPTED_QUALITY_THRESHOLD = "accepted_quality_threshold"
    REJECTED_DUPLICATE = "rejected_duplicate"
    REJECTED_EXCLUDED_SOURCE = "rejected_excluded_source"
    REJECTED_FACET_TARGET_MET = "rejected_facet_target_met"
    REJECTED_QUALITY_THRESHOLD = "rejected_quality_threshold"
    REJECTED_FETCH_FAILURE = "rejected_fetch_failure"


class ResearchFetchOutcome(StrEnum):
    FETCHED = "fetched"
    CURATED_TEXT = "curated_text"
    FAILED = "failed"


class ResearchCoverageStatus(StrEnum):
    MET = "met"
    PARTIAL = "partial"
    EMPTY = "empty"
    OVERSUBSCRIBED = "oversubscribed"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"
