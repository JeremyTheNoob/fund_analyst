# 部署到 Hugging Face 指南

## 🚀 免费部署到 Hugging Face Spaces

Hugging Face 提供 **免费** 的 Streamlit 应用托管服务，非常适合你的基金分析工具。

---

## 步骤 1: 注册 Hugging Face 账号

1. 访问 https://huggingface.co
2. 点击右上角 "Sign Up" 注册账号
3. 验证邮箱

---

## 步骤 2: 创建新的 Space

1. 登录后，点击右上角 "+ New" 按钮
2. 选择 "New Space"
3. 填写信息：
   - **Owner**: 你的用户名
   - **Space name**: 例如 `fund-analysis`（会生成访问链接的一部分）
   - **License**: 其他
   - **SDK**: 选择 **Streamlit**
   - **Space hardware**: 选择 **CPU Basic**（免费）
4. 点击 "Create Space"

---

## 步骤 3: 上传代码

### 方法 1: 通过网页上传（最简单）

1. 在新创建的 Space 页面，点击 "Files" 标签
2. 点击 "Upload files" 按钮
3. 依次上传以下文件：
   - `requirements.txt`
   - `fund_analysis.py`
   - `README.md`
4. 等待上传完成

### 方法 2: 通过 Git 命令（推荐）

如果你熟悉 Git，可以克隆后推送代码：

```bash
# 克隆你的 Space（替换 USERNAME 和 SPACE_NAME）
git clone https://huggingface.co/spaces/USERNAME/fund-analysis

# 进入目录
cd fund-analysis

# 复制你的项目文件
cp /Users/liuweihua/WorkBuddy/基金穿透式分析/* .

# 提交并推送
git add .
git commit -m "Initial commit"
git push
```

---

## 步骤 4: 等待构建和启动

上传完成后，Hugging Face 会自动：
1. 安装 `requirements.txt` 中的依赖
2. 启动 Streamlit 应用
3. 构建完成页面会显示绿色的 "App is running"

这个过程通常需要 2-5 分钟。

---

## 步骤 5: 访问你的应用

1. 在 Space 页面顶部，找到链接：
   ```
   https://huggingface.co/spaces/USERNAME/fund-analysis
   ```
2. 或者在应用标签页看到直接的访问链接

**恭喜！你的基金分析工具现在已经上线了！** 🎉

---

## 📝 更新代码

当你修改了代码后：

### 通过网页更新：
1. 在 Files 页面点击对应文件右侧的编辑按钮
2. 修改内容后保存

### 通过 Git 更新：
```bash
cd fund-analysis
cp /Users/liuweihua/WorkBuddy/基金穿透式分析/fund_analysis.py .
git add .
git commit -m "Update code"
git push
```

---

## 💡 使用技巧

### 1. 查看日志
如果应用运行有问题，点击 Space 页面的 "Logs" 标签查看错误信息。

### 2. 更新依赖
如果需要添加新的 Python 包：
1. 修改 `requirements.txt`
2. 上传更新
3. Hugging Face 会自动重新安装依赖

### 3. 自定义域名（可选）
Hugging Face Spaces 提供自定义域名绑定（需要你自己的域名）。

---

## ⚠️ 注意事项

1. **免费额度限制**：
   - CPU Basic 每个月有约 50 小时的运行时长
   - 1000 PV/天完全在免费额度内

2. **首次启动较慢**：
   - 免费版本启动需要约 30 秒
   - 用户首次访问需要等待启动时间

3. **数据刷新**：
   - AkShare 每次都会实时获取最新数据
   - 如果需要缓存优化，可以添加本地缓存逻辑

---

## 🆚 其他免费部署选项

除了 Hugging Face，你还可以考虑：

| 平台 | 免费额度 | 部署难度 | 推荐度 |
|------|:-------:|:-------:|:-----:|
| **Hugging Face Spaces** | ~50小时/月 | ⭐ 最简单 | ⭐⭐⭐⭐⭐ |
| **Streamlit Cloud** | ~200小时/月 | ⭐⭐ 简单 | ⭐⭐⭐⭐ |
| **Render** | 持续运行（750h/月） | ⭐⭐⭐ 中等 | ⭐⭐⭐ |
| **Railway** | $5 免费额度 | ⭐⭐⭐ 中等 | ⭐⭐ |

**Hugging Face 对于 Streamlit 应用是最友好的选择。**

---

## 🎉 完成！

按照上述步骤，你就可以免费把基金分析工具部署到互联网上了。如果遇到问题，可以随时联系我！
