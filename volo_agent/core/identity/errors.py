from __future__ import annotations


class UserServiceError(ValueError):
    def __init__(self, user_message: str, *, audit_detail: str):
        super().__init__(user_message)
        self.user_message = user_message
        self.audit_detail = audit_detail


class LinkAccountError(UserServiceError):
    pass


class InvalidLinkTokenError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "That link code isn't valid. Request a new code on the platform "
                "where your wallet already exists, then send 'link <CODE>' here."
            ),
            audit_detail="invalid_token",
        )


class ExpiredLinkTokenError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "That link code has expired. Request a new code on the platform "
                "where your wallet already exists, then send 'link <CODE>' here."
            ),
            audit_detail="expired_token",
        )


class UsedLinkTokenError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "That link code has already been used. Request a fresh code on "
                "the platform where your wallet already exists, then try again."
            ),
            audit_detail="used_token",
        )


class RevokedLinkTokenError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "That link code has been replaced by a newer one. Request a new "
                "code on the platform where your wallet already exists, then try again."
            ),
            audit_detail="revoked_token",
        )


class IdentityConflictError(LinkAccountError):
    def __init__(self, provider: str, provider_user_id: str):
        super().__init__(
            (
                f"This {provider} account ({provider_user_id}) is already linked "
                "to a different wallet. Unlink it there first or request a new "
                "code from the correct wallet."
            ),
            audit_detail="identity_conflict",
        )


class LinkTargetMissingError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "We found the link code, but couldn't find the wallet it belongs "
                "to. Request a new code on the original platform and try again."
            ),
            audit_detail="target_user_missing",
        )


class LinkAttachError(LinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "We couldn't finish linking this account right now. Request a new "
                "link code and try again."
            ),
            audit_detail="attach_failed",
        )


class UnlinkAccountError(UserServiceError):
    pass


class IdentityNotFoundError(UnlinkAccountError):
    def __init__(self):
        super().__init__(
            "That account isn't linked here.",
            audit_detail="identity_not_found",
        )


class LastIdentityError(UnlinkAccountError):
    def __init__(self):
        super().__init__(
            (
                "You can't unlink the last remaining account. Link another "
                "platform first, then try again."
            ),
            audit_detail="unlink_last_identity",
        )
