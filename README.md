# TG-Atlas

واجهة ويب ثابتة للبحث والاكتشاف في Telegram، مع محرك Telethon محلي اختياري يعمل على Android/Termux أو Linux/macOS/Windows.

## الموقع
- Repository: https://github.com/galaxyuniversity900-oss/tg-search-
- GitHub Pages: النشر تلقائي عبر GitHub Actions. إذا لم يكن Pages مفعّلًا، اختر Settings ثم Pages ثم GitHub Actions كمصدر للنشر.

## التشغيل
الواجهة العامة تعمل على GitHub Pages. البحث الحقيقي وتسجيل الدخول برقم الهاتف ورمز Telegram والتحقق بخطوتين يتم عبر server/tg_atlas.py باستخدام Telethon على جهازك؛ GitHub Pages لا يشغّل Python ولا يخزّن جلسة حسابك.

Android/Termux: نزّل tg_atlas.py وsetup_termux.sh إلى مجلد واحد، ثم نفّذ sh setup_termux.sh وبعدها atlas. افتح الرابط المحلي الذي يطبعه الأمر. أدخل api_id وapi_hash من https://my.telegram.org ثم رقم الهاتف ورمز التحقق وكلمة مرور 2FA عند طلبها. الإيقاف: atlas stop. السجل: atlas log.

البحث يستهدف المحتوى العام الذي تسمح به Telegram API. لا يضمن استرجاع كل القنوات أو المنشورات؛ Telegram يفرض حدودًا ومعدلات استخدام.

## الأمان
لا ترفع tg_atlas_config.json الحقيقي أو ملفات *.session أو قاعدة البيانات إلى GitHub. لا تشارك رمز Telegram أو كلمة مرور 2FA. احتفظ بملفات الجلسة محليًا. الخادم يستمع إلى 127.0.0.1 فقط وليس خادمًا عامًا.
