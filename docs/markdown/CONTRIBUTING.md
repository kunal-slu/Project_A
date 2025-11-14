# Contributing Guide

Thank you for contributing to the PySpark Data Engineering Platform!

## Branching Strategy

We follow a Git Flow-inspired branching strategy:

- **main**: Production-ready code
- **develop**: Development integration branch
- **feature/***: Feature development branches
- **hotfix/***: Critical production fixes
- **release/***: Release preparation branches

### Workflow

1. Create a feature branch from `develop`:
   ```bash
   git checkout develop
   git pull origin develop
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit:
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```

3. Push and create a pull request:
   ```bash
   git push origin feature/your-feature-name
   ```

## Commit Style

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting)
- **refactor**: Code refactoring
- **test**: Adding or updating tests
- **chore**: Maintenance tasks

### Examples

```
feat: add Delta Lake OPTIMIZE job
fix: resolve schema validation issue in bronze ingestion
docs: update deployment guide
test: add quality gate tests
```

## Code Style

### Python
- Follow PEP 8
- Use `black` for formatting (100 char line length)
- Use `ruff` for linting
- Type hints encouraged (mypy compliance)

### Configuration
- YAML files use 2-space indentation
- JSON files use 2-space indentation
- Validate configs before committing

### Testing

- **Unit tests**: Required for new features
- **Integration tests**: Required for ETL jobs
- **Quality gate tests**: Must pass for deployment
- Aim for >80% code coverage

Run tests:
```bash
make test        # All tests
make unit        # Unit tests only
make it          # Integration tests only
```

## Pull Request Process

1. **Ensure tests pass**: `make test`
2. **Ensure linting passes**: `make lint`
3. **Update documentation**: If changing behavior
4. **Update CHANGELOG.md**: For user-facing changes
5. **Request review**: From at least one reviewer

### PR Checklist

- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Linting passes
- [ ] No secrets committed
- [ ] Config schema validated

## Development Setup

1. **Clone repository**:
   ```bash
   git clone <repo-url>
   cd pyspark_data_engineer_project
   ```

2. **Create virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Set up pre-commit hooks**:
   ```bash
   pre-commit install
   ```

5. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your local settings
   ```

## Code Review Guidelines

### For Authors
- Keep PRs focused and reasonably sized
- Respond to feedback promptly
- Update PR based on comments

### For Reviewers
- Be constructive and respectful
- Review within 2 business days
- Focus on code quality, not style (use linters)
- Approve if changes look good

## Questions?

- **Documentation**: Check `docs/` directory
- **Issues**: Open a GitHub issue
- **Discussions**: Use GitHub Discussions

---

**Last Updated**: 2024-01-15

