"""
Shared HTML-to-text conversion utilities for ETL scrapers.
Preserves list formatting from HTML <ul>/<ol>/<li> elements.
"""

from bs4 import BeautifulSoup, NavigableString, Tag
from typing import List


def html_elem_to_text(elem) -> str:
    """
    Convert an HTML element to text, preserving list formatting.
    
    - <li> items become "- item text"
    - <p> items become plain paragraphs
    - Nested lists are flattened with indentation
    """
    if isinstance(elem, NavigableString):
        return str(elem).strip()
    
    if not isinstance(elem, Tag):
        return ""
    
    if elem.name == "li":
        # Get inner text, handling nested elements but not nested lists
        inner_parts = []
        for child in elem.children:
            if isinstance(child, Tag) and child.name in ("ul", "ol"):
                # Nested list: recurse with indent
                for li in child.find_all("li", recursive=False):
                    inner_parts.append("  - " + li.get_text(strip=True))
            elif isinstance(child, Tag):
                inner_parts.append(child.get_text(strip=True))
            else:
                text = str(child).strip()
                if text:
                    inner_parts.append(text)
        return "- " + " ".join(p for p in inner_parts if p and not p.startswith("  -")) + \
               ("\n" + "\n".join(p for p in inner_parts if p.startswith("  -")) if any(p.startswith("  -") for p in inner_parts) else "")
    
    if elem.name in ("ul", "ol"):
        items = []
        for li in elem.find_all("li", recursive=False):
            items.append(html_elem_to_text(li))
        return "\n".join(items)
    
    if elem.name in ("p", "div", "span", "td", "th"):
        return elem.get_text(separator=" ", strip=True)
    
    if elem.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        return elem.get_text(strip=True)
    
    return elem.get_text(separator=" ", strip=True)


def extract_body_with_lists(container, heading_tags=("h1", "h2", "h3", "h4")) -> List[dict]:
    """
    Extract sections from an HTML container, preserving list formatting.
    
    Returns list of {"section_title": ..., "body": ...} dicts.
    Each section body preserves <li> items as "- item" lines.
    """
    sections = []
    current_section = "Overview"
    current_parts = []
    
    BOILERPLATE = {"review questions", "references", "continuing education activity",
                   "copyright", "disclaimer"}
    
    def _flush():
        body = "\n".join(current_parts).strip()
        if body and len(body) > 50:
            if current_section.lower() not in BOILERPLATE:
                sections.append({
                    "section_title": current_section,
                    "body": body,
                })
    
    for elem in container.children:
        if not isinstance(elem, Tag):
            continue
        
        if elem.name in heading_tags:
            _flush()
            current_section = elem.get_text(strip=True)
            current_parts = []
        
        elif elem.name in ("ul", "ol"):
            # Preserve list formatting
            for li in elem.find_all("li", recursive=False):
                text = html_elem_to_text(li)
                if text and len(text.strip("- ")) > 5:
                    current_parts.append(text)
        
        elif elem.name == "li":
            text = html_elem_to_text(elem)
            if text and len(text.strip("- ")) > 5:
                current_parts.append(text)
        
        elif elem.name in ("p", "div"):
            # Check for nested lists inside <p> or <div>
            inner_lists = elem.find_all(["ul", "ol"])
            if inner_lists:
                # Get text before the list
                pre_text = ""
                for child in elem.children:
                    if isinstance(child, Tag) and child.name in ("ul", "ol"):
                        break
                    elif isinstance(child, Tag):
                        pre_text += child.get_text(strip=True) + " "
                    else:
                        pre_text += str(child).strip() + " "
                pre_text = pre_text.strip()
                if pre_text and len(pre_text) > 10:
                    current_parts.append(pre_text)
                # Now process the lists
                for ul in inner_lists:
                    for li in ul.find_all("li", recursive=False):
                        text = html_elem_to_text(li)
                        if text and len(text.strip("- ")) > 5:
                            current_parts.append(text)
            else:
                text = elem.get_text(separator=" ", strip=True)
                if text and len(text) > 10:
                    current_parts.append(text)
        
        elif elem.name == "table":
            # Tables: extract rows as text
            for row in elem.find_all("tr"):
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                row_text = " | ".join(c for c in cells if c)
                if row_text and len(row_text) > 10:
                    current_parts.append(row_text)
    
    _flush()
    return sections


def clean_html_preserve_lists(html_text: str) -> str:
    """
    Clean HTML text while preserving list formatting.
    Converts <ul><li> into "- item" bullet points.
    """
    if not html_text:
        return ""
    
    soup = BeautifulSoup(html_text, "html.parser")
    parts = []
    
    for elem in soup.children:
        if isinstance(elem, NavigableString):
            text = str(elem).strip()
            if text:
                parts.append(text)
        elif isinstance(elem, Tag):
            if elem.name in ("ul", "ol"):
                for li in elem.find_all("li", recursive=False):
                    parts.append(html_elem_to_text(li))
            elif elem.name == "li":
                parts.append(html_elem_to_text(elem))
            else:
                # Check for nested lists
                inner_lists = elem.find_all(["ul", "ol"])
                if inner_lists:
                    # Text before lists
                    direct_text = []
                    for child in elem.children:
                        if isinstance(child, Tag) and child.name in ("ul", "ol"):
                            for li in child.find_all("li", recursive=False):
                                direct_text.append(html_elem_to_text(li))
                        elif isinstance(child, Tag):
                            t = child.get_text(strip=True)
                            if t:
                                direct_text.append(t)
                        else:
                            t = str(child).strip()
                            if t:
                                direct_text.append(t)
                    parts.extend(direct_text)
                else:
                    text = elem.get_text(separator=" ", strip=True)
                    if text:
                        parts.append(text)
    
    import re
    result = "\n".join(parts)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()
