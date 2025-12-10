"""CSS стили для HTML дашборда"""


def get_css_styles() -> str:
    """
    Возвращает CSS стили для дашборда
    
    Returns:
        Строка с CSS кодом
    """
    return """
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            border-radius: 16px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .header h1 {
            color: #2d3748;
            font-size: 32px;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #718096;
            font-size: 16px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background: white;
            border-radius: 16px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
        }
        
        .stat-label {
            color: #718096;
            font-size: 14px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }
        
        .stat-value {
            color: #2d3748;
            font-size: 36px;
            font-weight: bold;
        }
        
        .stat-subtext {
            color: #a0aec0;
            font-size: 14px;
            margin-top: 5px;
        }
        
        .section {
            background: white;
            border-radius: 16px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .section-title {
            color: #2d3748;
            font-size: 24px;
            font-weight: bold;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .section-title::before {
            content: '';
            width: 4px;
            height: 24px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 2px;
        }
        
        .cluster-card {
            background: #f7fafc;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 15px;
            border: 2px solid #e2e8f0;
            transition: all 0.3s;
        }
        
        .cluster-card:hover {
            border-color: #667eea;
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
        }
        
        .cluster-header {
            display: flex;
            justify-content: space-between;
            align-items: start;
            margin-bottom: 15px;
            gap: 20px;
        }
        
        .cluster-name {
            font-size: 20px;
            font-weight: bold;
            color: #2d3748;
            flex: 1;
        }
        
        .cluster-stats {
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
        }
        
        .cluster-stat {
            text-align: center;
        }
        
        .cluster-stat-value {
            font-size: 24px;
            font-weight: bold;
            color: #667eea;
        }
        
        .cluster-stat-label {
            font-size: 12px;
            color: #718096;
            text-transform: uppercase;
        }
        
        .cluster-meta {
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
            flex-wrap: wrap;
        }
        
        .badge {
            display: inline-block;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 500;
        }
        
        .badge-intent {
            background: #e6fffa;
            color: #047857;
        }
        
        .badge-funnel {
            background: #fef3c7;
            color: #92400e;
        }
        
        .badge-url {
            background: #dbeafe;
            color: #1e40af;
            font-family: monospace;
        }
        
        .queries-list {
            background: white;
            border-radius: 8px;
            padding: 15px;
        }
        
        .query-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
            border-bottom: 1px solid #e2e8f0;
        }
        
        .query-item:last-child {
            border-bottom: none;
        }
        
        .query-text {
            color: #2d3748;
            font-size: 14px;
        }
        
        .query-freq {
            color: #667eea;
            font-weight: bold;
            font-size: 14px;
        }
        
        .distribution {
            display: grid;
            gap: 10px;
            margin-top: 15px;
        }
        
        .dist-item {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .dist-label {
            min-width: 150px;
            font-size: 14px;
            color: #4a5568;
        }
        
        .dist-bar {
            flex: 1;
            height: 30px;
            background: #e2e8f0;
            border-radius: 6px;
            overflow: hidden;
            position: relative;
        }
        
        .dist-fill {
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            transition: width 0.5s ease;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 10px;
        }
        
        .dist-value {
            color: white;
            font-weight: bold;
            font-size: 12px;
        }
        
        .toggle-btn {
            background: #667eea;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 500;
            transition: background 0.3s;
        }
        
        .toggle-btn:hover {
            background: #5568d3;
        }
        
        .queries-hidden {
            display: none;
        }
        
        .search-box {
            width: 100%;
            padding: 12px 20px;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-size: 16px;
            margin-bottom: 20px;
            transition: border-color 0.3s;
        }
        
        .search-box:focus {
            outline: none;
            border-color: #667eea;
        }
        
        .hidden {
            display: none !important;
        }
    """

