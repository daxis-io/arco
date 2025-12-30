# Security Policy

The Arco maintainers take security seriously. We appreciate your efforts to responsibly disclose your findings.

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.x.x   | :white_check_mark: |

Once we reach 1.0, we will maintain security updates for the current major version and one prior major version.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

### How to Report

Send a detailed report to: **security@daxis.io**

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fixes (optional)

### What to Expect

1. **Acknowledgment**: Within 48 hours of your report
2. **Initial Assessment**: Within 5 business days
3. **Status Updates**: Every 7 days until resolution
4. **Resolution**: Target within 90 days for critical issues

### After Resolution

- We will coordinate disclosure timing with you
- You will be credited in the security advisory (unless you prefer anonymity)
- We may offer a bounty for significant findings (at our discretion)

## Security Measures

### Supply Chain Security

- Dependency policy enforced via `cargo-deny` (licenses/sources/bans)
- License allowlist enforced via `cargo-deny` (with explicit exceptions)
- Automated vulnerability scanning via `cargo-deny advisories` and scheduled `pip-audit`
- SBOM generation for releases via `cargo sbom` (SPDX + CycloneDX)

### Code Security

- No `unsafe` code without explicit review and documentation
- Memory safety enforced through Rust's ownership model
- All cryptographic operations use audited libraries
- Input validation at all public API boundaries

### Infrastructure Security

- Multi-tenant isolation enforced at storage and service layers
- No credentials in code or configuration files
- Secrets managed through environment variables or secret managers
- Audit logging for security-relevant operations

## Security Best Practices for Users

### Deployment

1. Run with minimal required permissions
2. Enable TLS for all network communications
3. Use dedicated service accounts
4. Implement network segmentation
5. Enable audit logging

### Configuration

1. Never commit credentials to version control
2. Use environment variables or secret managers
3. Rotate credentials regularly
4. Review access permissions periodically

### Monitoring

1. Monitor for unusual access patterns
2. Set up alerts for failed authentication attempts
3. Review audit logs regularly
4. Keep dependencies updated

## Vulnerability Disclosure Policy

We follow a coordinated disclosure approach:

1. **Private Report**: Vulnerability reported privately
2. **Verification**: We verify and assess the issue
3. **Fix Development**: We develop and test a fix
4. **Release**: Fix released with security advisory
5. **Public Disclosure**: Details published after users have time to update

We request a 90-day disclosure window for critical vulnerabilities to allow users adequate time to patch.

## Security Advisories

Security advisories are published via:
- [GitHub Security Advisories](https://github.com/daxis-io/arco/security/advisories)
- Release notes
- Direct notification to known enterprise users

## Contact

- **Security Issues**: security@daxis.io
- **General Questions**: See [CONTRIBUTING.md](CONTRIBUTING.md)

Thank you for helping keep Arco and its users safe!
